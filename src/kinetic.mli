module Kinetic : sig
    type session
    type batch
    type connection = Lwt_io.input_channel * Lwt_io.output_channel

    type key = bytes
    type value = bytes
    type version = bytes option

    type entry = {
        key: key;
        db_version: version;
        new_version: version;
        vo : value option;
      }

    type rc
    type handler = rc -> unit Lwt.t
    exception Kinetic_exc of (int * bytes)

    val make_entry :
      key:key ->
      db_version:version ->
      new_version:version ->
      value option -> entry

    val handshake : string -> int64 -> connection -> session Lwt.t

    val put: session -> connection ->
             key -> value
             -> db_version:version
             -> new_version:version
             -> forced:bool option
             -> unit Lwt.t

    val delete_forced: session -> connection ->
                key -> unit Lwt.t

    val get : session -> connection -> key -> (value * version) option Lwt.t

    (*val noop: session -> connection -> unit Lwt.t *)

    val get_key_range: session -> connection ->
                       key -> bool ->
                       key -> bool ->
                       bool -> int ->
                       key list Lwt.t

   val start_batch_operation :
     ?handler:handler ->
     session -> connection -> int32 -> batch Lwt.t

   val batch_put :
     ?handler:handler ->
     batch -> entry -> forced:bool option
     -> unit Lwt.t

   val batch_delete:
     ?handler:handler ->
     batch -> entry -> forced: bool option
     -> unit Lwt.t

   val end_batch_operation :
     ?handler:handler ->
     batch -> connection Lwt.t

  (*
    val p2p_push : session -> connection ->
                   (string * int * bool) ->
                   (string * string option) list ->
                   unit Lwt.t
   *)
  end
