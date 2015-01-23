OCAML_LIBDIR ?= `ocamlfind printconf destdir`
OCAML_FIND ?= ocamlfind

all: piqi kinetic_piqi_ml example lib

piqi:
	piqi of-proto src/kinetic.proto -o src/kinetic.proto.piqi

kinetic_piqi_ml: piqi
	piqic-ocaml --embed-piqi -C src src/kinetic.proto.piqi

example:
	ocamlbuild -use-ocamlfind test_it.native

lib:
	ocamlbuild -use-ocamlfind \
	  kinetic.client.cma kinetic.client.cmxa kinetic.client.a


install-lib: lib
	mkdir -p $(OCAML_LIBDIR)
	$(OCAML_FIND) install kinetic-client -destdir $(OCAML_LIBDIR) META \
	  _build/src/kinetic.mli \
	  _build/src/kinetic.cmi \
	  _build/src/kinetic.cmx \
          _build/src/kinetic_piqi.cmx \
	  _build/src/piqirun.cmx \
	  _build/src/kinetic.client.a \
	  _build/src/kinetic.client.cma \
	  _build/src/kinetic.client.cmxa

uninstall-lib:
	$(OCAML_FIND) remove kinetic-client -destdir $(OCAML_LIBDIR)

clean :
	rm -f src/kinetic.proto.piqi
	rm -f src/kinetic_piqi.ml
	rm -rf _build
	rm -f test_it.native
