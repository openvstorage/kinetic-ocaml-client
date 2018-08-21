OCAML_LIBDIR ?= `ocamlfind printconf destdir`
OCAML_FIND ?= ocamlfind

all: proto example lib

proto:
	ocaml-protoc -binary -ml_out ./src ./src/kinetic.proto

example: proto
	ocamlbuild -use-ocamlfind test_it.native

lib: proto
	ocamlbuild -use-ocamlfind \
	  kinetic.client.cma kinetic.client.cmxa kinetic.client.a


install-lib: lib
	mkdir -p $(OCAML_LIBDIR)
	$(OCAML_FIND) install kinetic-client -destdir $(OCAML_LIBDIR) META \
	  _build/src/kinetic.mli \
	  _build/src/kinetic.cmi \
	  _build/src/kinetic.cmx \
	  _build/src/kinetic_types.cmi \
	  _build/src/kinetic_types.cmx \
	  _build/src/kinetic_pb.cmi \
	  _build/src/kinetic_pb.cmx \
	  _build/src/kinetic.client.a \
	  _build/src/kinetic.client.cma \
	  _build/src/kinetic.client.cmxa

uninstall-lib:
	$(OCAML_FIND) remove kinetic-client -destdir $(OCAML_LIBDIR)

clean :
	rm -f ./src/kinetic_types.ml
	rm -f ./src/kinetic_types.mli
	rm -f ./src/kinetic_types.ml
	rm -f ./src/kinetic_pb.mli
	rm -f ./src/kinetic_pb.ml
	rm -rf _build
	rm -f test_it.native
