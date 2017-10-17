module Config: sig
    type t = {
        vendor: string;
        model:string;
        serial_number: string;
        world_wide_name: string;
        version: string;
        ipv4_addresses: string list;
        (* limits *)
        max_key_size:int;
        max_value_size: int;
        max_version_size:int;
        max_tag_size: int;
        max_connections: int;
        max_outstanding_read_requests: int;
        max_outstanding_write_requests: int;
        max_message_size: int;
        max_key_range_count: int;
        (* are in protocol definition but device doesn't send them *)
        (* max_operation_count_per_batch: int; *)
        (* max_batch_count_per_device: int; *)
      }
    (*

    val make : vendor:string ->
               model:string ->
               serial_number:string ->
               world_wide_name:string ->
               version: string -> t
     *)
    val show : t -> string
  end

module Kinetic : sig
    type session

    val get_connection_id : session -> int64


    type batch
    val get_batch_id : batch -> int32

    type connection = Lwt_io.input_channel * Lwt_io.output_channel

    type client

    val get_session : client -> session

    type key = bytes
    type value = bytes
    type version = bytes option

    type tag =
      | Invalid of Bytes.t
      | Sha1 of Bytes.t
      | Crc32 of int32

    type entry = {
        key: key;
        db_version: version;
        new_version: version;
        vt : (value * tag )option;
      }

    val show_entry: entry -> string

    type synchronization =
      | WRITETHROUGH
      | WRITEBACK
      | FLUSH



    type rc
    type handler = rc -> unit Lwt.t
    exception Kinetic_exc of (int * bytes) list

    val convert_rc : rc -> (int * bytes) option

    val get_config : session -> Config.t

    val make_entry :
      key:key ->
      db_version:version ->
      new_version:version ->
      (value * tag) option ->
      entry

    val make_sha1  : value -> tag
    val make_crc32 : value -> tag

    (** The initial contact with the device, which
        will send some information that is needed in the session
      *)

    val handshake : string -> int64 -> ?trace:bool
                    -> connection -> session Lwt.t

    (** turn on or of the trace logging of the raw messages

     *)
    val tracing : session -> bool -> unit
    (** insert a key value pair.
        db_version is the version that's supposed to be the current version
        in the database.
        new_version is the version of the key value pair _after_ the update.
        forced updates happen regardless the db_version
     *)

    val put: client ->
             key -> value
             -> db_version:version
             -> new_version:version
             -> forced:bool option
             -> synchronization : synchronization option
             -> tag: tag option
             -> unit Lwt.t

    val delete_forced: client -> key -> unit Lwt.t

    val get : client -> key -> (value * version) option Lwt.t

    val noop: client -> unit Lwt.t

    val instant_secure_erase: ?pin:string -> client -> unit Lwt.t

    val download_firmware: client -> string -> unit Lwt.t

    val get_key_range: client ->
                       key -> bool ->
                       key -> bool ->
                       bool -> int ->
                       key list Lwt.t

    (** returns capacity of the drive and portion full
     *)
    val get_capacities : client -> (int64 * float) Lwt.t

   (**
       Batches are atomic multi-updates.
       Remark:
       - while you're doing a batch, you're not supposed to use the client
         for other things
       - handlers should not raise exceptions as these have nowhere to go.
    *)
   val start_batch_operation : ?handler:handler -> client -> batch Lwt.t

   val batch_put  :  batch -> entry -> forced:bool option -> unit Lwt.t

   val batch_delete: batch -> entry -> forced:bool option -> unit Lwt.t

   val end_batch_operation :
     ?handler:handler ->
     batch -> (bool * connection) Lwt.t

  (* (* we might need it again in the future *)
    val p2p_push : session -> connection ->
                   (string * int * bool) ->
                   (string * string option) list ->
                   unit Lwt.t
   *)

   val make_client:
     ?ctx:Ssl.context ->
     ?secret:string ->
     ?cluster_version:int64 ->
     ?trace:bool ->
     ip:string -> port:int ->
     client Lwt.t

   val dispose : client -> unit Lwt.t

   val wrap_connection :
     ?trace:bool ->
     ?secret:string ->
     ?cluster_version:int64 ->
     connection ->
     (unit -> unit Lwt.t)
     -> client Lwt.t


   val with_client :
     ?ctx:Ssl.context ->
     ?secret:string ->
     ?cluster_version:int64 ->
     ?trace:bool ->
     ip:string -> port:int ->
     (client -> 'a Lwt.t) -> 'a Lwt.t
  end
