.PHONY: all docs sphinx format

sphinx:
	cd docs && make html

format:
	ruff check examples --fix
	ruff format examples
	isort .

rm-docs:
	rm -rf docs/source/gallery
	rm -rf docs/html

add-path:
	export PYTHONPATH="./:${PYTHONPATH}"
 
docs: format sphinx
docs-clean: rm-docs docs