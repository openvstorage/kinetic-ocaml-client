module Kinetic : sig
    type session

    type connection = Lwt_io.input_channel * Lwt_io.output_channel
    type key = bytes
    type value = bytes

    val make_session :
      ?cluster_version:int64 ->
      ?identity:int64 ->
      ?sequence:int64 ->
      secret:string ->
      connection_id:int64 ->
      session

    val set : session -> connection -> key -> value option -> unit Lwt.t
    val get : session -> connection -> key -> value option Lwt.t
    val noop: session -> connection -> unit Lwt.t

    val get_key_range: session -> connection ->
                       key -> bool ->
                       key -> bool ->
                       bool -> int ->
                       key list Lwt.t


    val p2p_push : session -> connection ->
                   (string * int * bool) ->
                   (string * string option) list ->
                   unit Lwt.t
end
