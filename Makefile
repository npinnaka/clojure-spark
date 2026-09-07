.PHONY: help deps test clean

LEIN ?= lein

help:
	@printf "%s\n" \
		"make deps   Download project dependencies" \
		"make test   Run the repository validation command" \
		"make clean  Remove generated build artifacts"

deps:
	$(LEIN) deps

test:
	$(LEIN) test

clean:
	$(LEIN) clean
