tag = v0.0.2

build:
	git commit -am "tag $(tag)" && git push || true
	git tag $(tag)
	git push origin $(tag)


.PHONY: build