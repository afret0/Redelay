tag = v1.5.71

build:
	git commit -am "f" && git push || true
	git tag $(tag)
	git push origin $(tag)


.PHONY: build