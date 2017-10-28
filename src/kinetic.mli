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

type off = int
type len = int
type 'a slice = 'a * off * len

type key = bytes
type version = bytes option
             


module Tag : sig
  type t =
    | Invalid of Bytes.t
    | Sha1 of Bytes.t
    | Crc32 of int32
end

module type INTEGRATION = sig
  type value

  type socket 
  val create : int -> value
  val show : value -> string

  (*  the value type can be chosen by the integrator ... *)

  val read  : socket -> value -> off -> len -> int Lwt.t
  val write : socket -> value -> off -> len -> int Lwt.t

  (* but, the protocol uses bytes because of piqi *)
  val read_bytes  : socket -> Bytes.t -> off -> len -> int Lwt.t
  val write_bytes : socket -> Bytes.t -> off -> len -> int Lwt.t

  val make_sha1  : value -> off -> len -> Tag.t
  val make_crc32 : value -> off -> len -> Tag.t

end

module BytesIntegration : sig
  type value = Bytes.t
  type socket = Lwt_ssl.socket
  val create : int -> value
  val show : value -> string
  val read  : socket -> value -> off -> len -> int Lwt.t
  val write : socket -> value -> off -> len -> int Lwt.t

  val read_bytes : socket -> Bytes.t -> off -> len -> int Lwt.t
  val write_bytes : socket -> Bytes.t -> off -> len -> int Lwt.t
    
  val make_sha1  : value -> off -> len -> Tag.t
  val make_crc32 : value -> off -> len -> Tag.t
end

module Make(I:INTEGRATION) : sig

  module Entry : sig
    type t = {
        key: key;
        db_version: version;
        new_version: version;
        vt : (I.value slice * Tag.t )option;
      }

    val make :
      key:key ->
      db_version:version ->
      new_version:version ->
      (I.value slice * Tag.t) option ->
      t
      
    val show: t -> string
  end
       
  type session

  val get_connection_id : session -> int64

  type batch

  val get_batch_id : batch -> int32

  type client

  val get_session : client -> session

  type synchronization =
    | WRITETHROUGH
    | WRITEBACK
    | FLUSH

  type rc
  type handler = rc -> unit Lwt.t
               
  exception Kinetic_exc of (int * bytes) list

  val convert_rc : rc -> (int * bytes) option

  val get_config : session -> Config.t



  val make_sha1  : I.value slice -> Tag.t
  val make_crc32 : I.value slice -> Tag.t

  (** The initial contact with the device, which
        will send some information that is needed in the session
   *)

  val handshake : string -> int64 -> ?trace:bool
                  -> I.socket -> session Lwt.t

  (** turn on or of the trace logging of the raw messages

   *)
  val tracing : session -> bool -> unit
  (** insert a key value pair.
        db_version is the version that's supposed to be the current version
        in the database.
        new_version is the version of the key value pair _after_ the update.
        forced updates happen regardless the db_version
   *)

  val put:
    client ->
    key -> I.value slice
    -> db_version:version
    -> new_version:version
    -> forced:bool option
    -> synchronization: synchronization option
    -> tag: Tag.t option
    -> unit Lwt.t

  val delete_forced: client -> key -> unit Lwt.t

  val get : client -> key -> (I.value * version) option Lwt.t

  val noop: client -> unit Lwt.t

  val instant_secure_erase: ?pin:string -> client -> unit Lwt.t

  val download_firmware: client -> I.value slice -> unit Lwt.t

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

  val batch_put  :  batch -> Entry.t -> forced:bool option -> unit Lwt.t

  val batch_delete: batch -> Entry.t -> forced:bool option -> unit Lwt.t

  val end_batch_operation :
    ?handler:handler ->
    batch -> (bool * I.socket) Lwt.t

  (* (* we might need it again in the future *)
    val p2p_push : session -> connection ->
                   (string * int * bool) ->
                   (string * string option) list ->
                   unit Lwt.t
   *)

  val wrap_socket :
    ?trace:bool ->
    ?secret:string ->
    ?cluster_version:int64 ->
    I.socket ->
    (unit -> unit Lwt.t)
    -> client Lwt.t
    
  val dispose : client -> unit Lwt.t

end
