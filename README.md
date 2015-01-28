Kinetic OCaml Client
====================
This is an OCaml client for [Seagate's Kinetic drives](https://developers.seagate.com/display/KV/Kinetic+Open+Storage+Documentation+Wiki).
Currently, it uses protocol version 3.0.6.
This is corresponds with version 0.8.0.3 of the Java Simulator.

Todo:
- [X] support 3.X protocol
- [ ] use 4.0.2 Bytes iso strings for buffers (depends on piqi)
- [X] opam installable
- [ ] publish 0.0.3 on opam repo

Installation
============
In order to build the client, you need to have some OCaml libraries present.
In concreto, you need:
  - Lwt
  - piqi
  - Cryptokit


If you have these, you can compile everything with:

```
$> make
```

You can install the library with:


```
$> make install-lib
```


Once you have the library installed, you just add `true:package(kinetic-client) to your ocamlbuild _tags file.

Usage
=====



The API is defined in [kinetic.mli](src/kinetic.mli)

typically you'd do something like:

```OCaml
    let sa = make_socket_address "127.0.0.1" 8123 in
    let secret = "...." in
    let cluster_version = ... in
    let session = Kinetic.make_session ....in
    Lwt_io.with_connection
      sa
      (fun conn ->
       Kinetic.handshake secret cluster_version conn >>= fun session ->
       ...
       Kinetic.put session
           conn "the_key"
           (Some "the value")
           ~db_version:None ~new_version:None
           ~forced:true
           >>= fun () ->
       ...
      )

```


Have fun,

   The CloudFounders team
