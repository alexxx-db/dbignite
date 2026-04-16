from abc import ABC, abstractmethod
import warnings, json
from multiprocessing.pool import ThreadPool
import multiprocessing as mp
from typing import Optional, cast
from dbignite.fhir_mapping_model import FhirSchemaModel
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    expr,
    filter,
    from_json,
    get_json_object,
    input_file_name,
    lit,
    nullif,
    sha2,
    size,
    sum,
    transform,
    trim,
    upper,
)
from pyspark.sql.types import ArrayType, StringType, StructType

#
#
#  BUNDLE_SCHEMA - lightweight representation of first level json schema
#
class FhirResource(ABC):
    BUNDLE_SCHEMA = (
        StructType()
         .add("resourceType", StringType())
         .add("entry", ArrayType(StructType().add("resource", StringType())))
         .add("id", StringType())
         .add("timestamp", StringType())
    )

    NDJSON_SCHEMA = (
        StructType()
         .add("resourceType", StringType())
    )
    
    @abstractmethod
    def __init__(self, raw_data: DataFrame) -> None:
        ...

    @abstractmethod
    def entry() -> DataFrame:
        ...
        
    @staticmethod
    @abstractmethod
    def resource_type() -> str:
        ...

    #
    # @param data - dataframe with new line delimited json FHIR resources
    # @return - new DBIgnite FHIR resource representation
    #
    @staticmethod
    def from_raw_ndjson_resource(data: DataFrame) -> "FhirResource":
        return BundleFhirResource(data, parser = "read_ndjson_data")

    
    #
    # @param data - dataframe with column "resource" that contains raw bundle json text in utf-8
    # @return - new DBIgnite FHIR resource representation
    #
    @staticmethod
    def from_raw_bundle_resource(data: DataFrame, streaming: bool = False) -> "FhirResource":
        resources_df = data.select(col("resource"), get_json_object("resource", "$.resourceType").alias("resourceType"))
        if not streaming:
            non_bundle = resources_df.filter("upper(resourceType) != 'BUNDLE'")
            skipped = non_bundle.count()
            if skipped > 0:
                warnings.warn(
                    f"Found {skipped} row(s) with resourceType other than Bundle; "
                    "only Bundle rows are read. Other rows are skipped.",
                    UserWarning,
                    stacklevel=2,
                )
            resources_df = resources_df.filter("upper(resourceType) == 'BUNDLE'")
        else:
            resources_df = resources_df.filter("upper(resourceType) == 'BUNDLE'")
        return BundleFhirResource(resources_df, streaming=streaming)

#
# Core representation of FHIR bundles
#  raw_data - raw json of FHIR bundle
#  entry - internal representation of entry schema for FHIR
#
class BundleFhirResource(FhirResource):

    #
    # @param - raw_data is a dataframe containing raw text resources
    # @param - parser = the function to use to build the _entry representation of resources
    #
    def __init__(self, raw_data, parser = None, streaming: bool = False) -> None:
        self._raw_data = raw_data
        self._entry = None
        self._parser = getattr(self, parser) if parser is not None else self.read_bundle_data
        self._streaming = streaming

    #
    # Main entry to the FHIR resources in a bundle
    #
    def entry(self, schemas = FhirSchemaModel()) -> DataFrame:
        if self._streaming:
            return self._parser(schemas=schemas)
        if self._entry is None:
            self._entry = self._parser(schemas=schemas)
        return self._entry

    #
    # Count across all bundles
    #
    def count_resource_type(
        self, resource_type: str, column_alias: str = "resource_sum"
    ) -> DataFrame:
        return self.entry().select(sum(size(col(resource_type))).alias(column_alias))

    #
    # Count within bundle
    #
    def count_within_bundle_resource_type(
        self, resource_type: str, column_alias: str = "resource_bundle_sum"
    ) -> DataFrame:
        return self.entry().select(size(col(resource_type)).alias(column_alias))


    #
    # reading ndjson data (DataFrame-only; supports streaming and bulk correlation column)
    #
    def read_ndjson_data(self, schemas):
        df = self._raw_data
        df = df.withColumn(
            "_resources",
            expr("transform(filter(split(resource, '\\n'), x -> trim(x) != ''), x -> trim(x))"),
        )
        corr = (
            col("bulkExportCorrelationId")
            if "bulkExportCorrelationId" in df.columns
            else lit(None).cast("string")
        )
        bundle_uuid = sha2(
            concat_ws(
                lit("|"),
                coalesce(corr, lit("")),
                coalesce(input_file_name(), lit("")),
                lit("ndjson"),
            ),
            256,
        )
        df = df.withColumn("bundleUUID", bundle_uuid)
        sel = (
            BundleFhirResource.list_entry_columns(schemas, parent_column=col("_resources"))
            + [lit("").alias("id"), lit("").alias("timestamp"), col("bundleUUID")]
        )
        if "bulkExportCorrelationId" in df.columns:
            sel.append(col("bulkExportCorrelationId"))
        return df.select(sel)

    #
    # Read and parse all data in raw_data dataframe
    #  @returns new dataframe with entry.x where x is an array of each resource provided
    #
    def read_bundle_data(self, schemas = FhirSchemaModel()) -> DataFrame:
        parsed = self._raw_data.select(
            from_json("resource", BundleFhirResource.BUNDLE_SCHEMA).alias("bundle"),
            col("resource").alias("_raw_resource"),
        )
        bundle_uuid = sha2(
            coalesce(
                nullif(trim(col("bundle.id")), lit("")),
                sha2(col("_raw_resource"), 256),
            ),
            256,
        )
        return (
            parsed.select(
                BundleFhirResource.list_entry_columns(schemas)
                + [col("bundle.timestamp"), col("bundle.id"), col("_raw_resource")]
            )
            .withColumn("bundleUUID", bundle_uuid)
            .drop("_raw_resource")
        )
    
    #
    # @param schemas - resources to parse out into separate columns with their associated spark schemas
    # @param parent_column - location of the list of resources 
    # @return a list of column transformations to select based upon the schemas provided
    #
    @staticmethod
    def list_entry_columns(schemas, parent_column=col("bundle.entry.resource")):
        return [
            BundleFhirResource.__convert_from_json(
                BundleFhirResource.__filter_resources(parent_column, resource_type),
                schema,
                resource_type,
            )
            for resource_type, schema in schemas.fhir_resource_map.items()
            if resource_type.upper() != "BUNDLE"
        ]
        
        
    #
    # Given a schema return a column mapped to the schema
    #  @param column - the column to parse json data from
    #  @param schema - the schema of the column to build
    #  @param resource_type - the FHIR resource being parsed (must be filtered prior to this step)
    #  @return a new column named resource_type with specified spark_schema
    #
    @staticmethod
    def __convert_from_json(column: Column,
                            schema: StructType,
                            resource_type: str) -> Column:
        if column is None:
            return lit(None)
        return transform(column, lambda x: from_json(x, schema)).alias(resource_type)

    #
    # "Bundle" resource representation
    #
    @staticmethod
    def resource_type() -> str:
        return "Bundle"

    #
    # Given a FHIR resource name, return all matching entries
    #  @param column - of the self.entry DF
    #  @param resource_type - FHIR resource name to parse
    #  @return all columns that match the resource type 
    #
    @staticmethod
    def __filter_resources(column: Column, resource_type: str) -> Column:
        return filter(
            column,
            lambda x: upper(get_json_object(x, "$.resourceType"))
            == lit(resource_type.upper()),
        )


    #
    # Bulk write resources as relational tables, 1 table per FHIR resource
    #  @param columns - list of columns to write, default None = write all in Dataframe
    #  @param location - catalog.database.schema definition of where to write
    #  @param write_mode - spark's write mode, default of append to existing tables
    #  @return None
    #
    def bulk_table_write(self, location = "",  write_mode = "append", columns = None):
        skip = {"id", "timestamp", "bundleUUID", "bulkExportCorrelationId"}
        cols = [c for c in self.entry().columns if c not in skip] if columns is None else columns
        pool = ThreadPool(min(4, max(1, mp.cpu_count() - 1)))
        try:
            pool.map(lambda column: self.table_write(str(column), location, write_mode), cols)
        finally:
            pool.close()
            pool.join()

    #
    # Write an individual FHIR resource as a table
    #
    def table_write(self, column, location = "", write_mode = "append"):
        e = self.entry()
        sel = [col("bundleUUID"), col("timestamp"), col("id"), column]
        if "bulkExportCorrelationId" in e.columns:
            sel.insert(1, col("bulkExportCorrelationId"))
        e.select(*sel).write.format("delta").mode(write_mode).saveAsTable((location + "." + column).lstrip("."))

    #
    # Returns a string representing ndjson for each grouping/bundle of FHIR resources
    #
    def get_ndjson_resources(self):
        pass

    #
    # Returns a string representing FHIR Bundles for each groupin gof resources
    #
    def get_bundle_resources(self):
        pass
