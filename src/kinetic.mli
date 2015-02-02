module Config: sig
    type t = {
        vendor: string;
        model:string;
        serial_number: string;
        world_wide_name: string;
        version: string;
        (* limits *)
        max_key_size:int;
        max_value_size: int;
        max_version_size:int;
      }
    (*

    val make : vendor:string ->
               model:string ->
               serial_number:string ->
               world_wide_name:string ->
               version: string -> t
     *)
  end

module Kinetic : sig
    type session
    val get_connection_id : session -> int64


    type batch
    val get_batch_id : batch -> int32

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
    val entry_to_string: entry -> string
    type rc
    type handler = rc -> unit Lwt.t
    exception Kinetic_exc of (int * bytes)

    val convert_rc : rc -> (int * bytes) option

    val get_config : session -> Config.t

    val make_entry :
      key:key ->
      db_version:version ->
      new_version:version ->
      value option -> entry

   (** The initial contact with the device.
       It will send some information that is needed in the session *)
    val handshake : string -> int64 -> connection -> session Lwt.t

    (** insert a key value pair.
        db_version is the version that's supposed to be the current version
        in the database.
        new_version is the version of the key value pair _after_ the update.
        forced updates happen regardless the db_version
     *)

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

   (**
       Batches are atomic multi-updates.
       (while you're doing a batch, you're not supposed to use the connection )
    *)
   val start_batch_operation :
     ?handler:handler ->
     session -> connection -> batch Lwt.t

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

  (* (* we might need it again in the future *)
    val p2p_push : session -> connection ->
                   (string * int * bool) ->
                   (string * string option) list ->
                   unit Lwt.t
   *)
  end
