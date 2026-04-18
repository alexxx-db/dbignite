# dbignite — build and deployment automation
#
# Usage:
#   make test          Run the test suite
#   make build         Build the wheel
#   make deploy        Build + upload to Databricks Volume
#   make release       Tag + push (triggers GitHub Actions release pipeline)
#   make clean         Remove build artifacts

PYTHON ?= python
VERSION := $(shell $(PYTHON) -c "import tomllib; print(tomllib.load(open('pyproject.toml','rb'))['project']['version'])")
WHEEL := dist/dbignite-$(VERSION)-py3-none-any.whl
CATALOG ?= hls_workshop
VOLUME_PATH ?= /Volumes/$(CATALOG)/libraries/wheels

.PHONY: test build clean deploy release install lint

# -----------------------------------------------------------------------
# Development
# -----------------------------------------------------------------------

install:  ## Install in editable mode with dev dependencies
	$(PYTHON) -m pip install -e ".[dev]"

test: install  ## Run the test suite
	SPARK_LOCAL_IP=127.0.0.1 \
	PYSPARK_PYTHON=$(PYTHON) \
	PYSPARK_DRIVER_PYTHON=$(PYTHON) \
	$(PYTHON) -m pytest tests/ -v --timeout=120

lint:  ## Check syntax across all source files
	$(PYTHON) -c "import ast, glob; [ast.parse(open(f).read()) or print(f'OK: {f}') for f in glob.glob('dbignite/**/*.py', recursive=True)]"

# -----------------------------------------------------------------------
# Build
# -----------------------------------------------------------------------

build: clean  ## Build wheel + sdist
	$(PYTHON) -m pip install build
	$(PYTHON) -m build
	@echo "Built: $(WHEEL)"

clean:  ## Remove build artifacts
	rm -rf dist/ build/ *.egg-info dbignite.egg-info dbignite/*.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

# -----------------------------------------------------------------------
# Deploy to Databricks
# -----------------------------------------------------------------------

deploy: build  ## Build and upload wheel to Databricks Volume
	@echo "Deploying $(WHEEL) to $(VOLUME_PATH)/"
	@if [ -z "$$DATABRICKS_HOST" ]; then \
		echo "Error: DATABRICKS_HOST not set"; \
		echo "  export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com"; \
		exit 1; \
	fi
	databricks fs mkdirs dbfs:$(VOLUME_PATH) 2>/dev/null || true
	databricks fs cp $(WHEEL) dbfs:$(VOLUME_PATH)/dbignite-$(VERSION)-py3-none-any.whl --overwrite
	@echo "Deployed: $(VOLUME_PATH)/dbignite-$(VERSION)-py3-none-any.whl"
	@echo ""
	@echo "Install in a notebook:"
	@echo "  %pip install $(VOLUME_PATH)/dbignite-$(VERSION)-py3-none-any.whl"

deploy-latest: deploy  ## Deploy and create a 'latest' symlink
	databricks fs cp $(WHEEL) dbfs:$(VOLUME_PATH)/dbignite-latest-py3-none-any.whl --overwrite
	@echo "Also deployed as: $(VOLUME_PATH)/dbignite-latest-py3-none-any.whl"

# -----------------------------------------------------------------------
# Release (tag-driven, triggers GitHub Actions)
# -----------------------------------------------------------------------

release: test build  ## Tag current version and push (triggers full release pipeline)
	@echo "Releasing v$(VERSION)..."
	@if git tag -l "v$(VERSION)" | grep -q "v$(VERSION)"; then \
		echo "Error: Tag v$(VERSION) already exists."; \
		echo "  Option 1: Bump version — make bump-patch (or bump-minor, bump-major)"; \
		echo "  Option 2: Use GitHub UI — Actions > Release > Run workflow"; \
		exit 1; \
	fi
	git tag -a "v$(VERSION)" -m "Release v$(VERSION)"
	git push origin "v$(VERSION)"
	@echo ""
	@echo "Tag v$(VERSION) pushed. GitHub Actions will:"
	@echo "  1. Test on Python 3.10 / 3.11 / 3.12"
	@echo "  2. Build wheel + sdist"
	@echo "  3. Generate changelog from commits"
	@echo "  4. Create GitHub Release with wheel + changelog"
	@echo "  5. Publish to PyPI (if PYPI_API_TOKEN secret is set)"
	@echo "  6. Deploy wheel to Databricks Volume"

bump-patch:  ## Bump patch version (0.2.4 → 0.2.5), commit, and release
	@$(PYTHON) -c "\
	import tomllib, re; \
	c = tomllib.load(open('pyproject.toml','rb')); \
	v = c['project']['version']; \
	parts = v.split('.'); \
	parts[2] = str(int(parts[2])+1); \
	nv = '.'.join(parts); \
	txt = open('pyproject.toml').read(); \
	open('pyproject.toml','w').write(txt.replace(f'version = \"{v}\"', f'version = \"{nv}\"')); \
	print(f'Bumped {v} → {nv}')"
	git add pyproject.toml
	git commit -m "Bump to v$(shell $(PYTHON) -c "import tomllib; print(tomllib.load(open('pyproject.toml','rb'))['project']['version'])")"
	$(MAKE) release

bump-minor:  ## Bump minor version (0.2.4 → 0.3.0), commit, and release
	@$(PYTHON) -c "\
	import tomllib, re; \
	c = tomllib.load(open('pyproject.toml','rb')); \
	v = c['project']['version']; \
	parts = v.split('.'); \
	parts[1] = str(int(parts[1])+1); parts[2] = '0'; \
	nv = '.'.join(parts); \
	txt = open('pyproject.toml').read(); \
	open('pyproject.toml','w').write(txt.replace(f'version = \"{v}\"', f'version = \"{nv}\"')); \
	print(f'Bumped {v} → {nv}')"
	git add pyproject.toml
	git commit -m "Bump to v$(shell $(PYTHON) -c "import tomllib; print(tomllib.load(open('pyproject.toml','rb'))['project']['version'])")"
	$(MAKE) release

bump-major:  ## Bump major version (0.2.4 → 1.0.0), commit, and release
	@$(PYTHON) -c "\
	import tomllib, re; \
	c = tomllib.load(open('pyproject.toml','rb')); \
	v = c['project']['version']; \
	parts = v.split('.'); \
	parts[0] = str(int(parts[0])+1); parts[1] = '0'; parts[2] = '0'; \
	nv = '.'.join(parts); \
	txt = open('pyproject.toml').read(); \
	open('pyproject.toml','w').write(txt.replace(f'version = \"{v}\"', f'version = \"{nv}\"')); \
	print(f'Bumped {v} → {nv}')"
	git add pyproject.toml
	git commit -m "Bump to v$(shell $(PYTHON) -c "import tomllib; print(tomllib.load(open('pyproject.toml','rb'))['project']['version'])")"
	$(MAKE) release

# -----------------------------------------------------------------------
# Help
# -----------------------------------------------------------------------

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
