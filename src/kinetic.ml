open Cryptokit

let _decode_fixed32 s off =
  let byte x = int_of_char s.[off + x] in
  ((byte 3) lsl  0)
  + ((byte 2) lsl  8)
  + ((byte 1) lsl 16)
  + ((byte 0) lsl 24)

let _encode_fixed32 (s:bytes) off i =
  let get_char i shift = char_of_int ((i land (0xff lsl shift)) lsr shift) in
  Bytes.set s off (get_char i 24);
  Bytes.set s (off + 1) (get_char i 16);
  Bytes.set s (off + 2) (get_char i  8);
  Bytes.set s (off + 3) (get_char i  0)

let calculate_hmac secret msg =
  let sx0 = Bytes.create 4 in
  _encode_fixed32 sx0 0 (String.length msg);
  let h = Cryptokit.MAC.hmac_sha1 secret in
  let () = h # add_string sx0 in
  let () = h # add_string msg in
  h # result

let to_hex s =
  let buf = Buffer.create (String.length s * 3) in
  let hex c = Printf.sprintf "%02x " (Char.code c) in
  String.iter (fun c -> Buffer.add_string buf (hex c)) s;
  Buffer.contents buf

let unwrap_option msg = function
  | None -> failwith ("None " ^ msg)
  | Some x -> x

open Kinetic_piqi
open Message
open Command
open Command_header
open Command_body
open Command_status

let _assert_type (command:Command.t) typ =
  let header  = unwrap_option "header" command.header in
  let htyp    = unwrap_option "message type" header.message_type in
  assert (htyp = typ)

let _get_status (command: Command.t) =
  let status = unwrap_option "status" command.status in
  status

let _get_status_code (status:Command_status.t) =
  let code = unwrap_option "status.code" status.code in
  code

let _get_status_message (status:Command_status.t) =
  let msg = unwrap_option "status.message" status.status_message in
  msg

let _assert_success (command:Command.t) =
  let code = _get_status command |> _get_status_code in
  assert (code = `success)



let maybe_verify_msg (m:Message.t) =
  let open Message in
  match m.auth_type with
  | Some `unsolicitedstatus -> ()
  | _ -> failwith "todo:verify_msg"

let verify_status (status:Command_status.t) =
  let open Command_status in
  let code = unwrap_option "status.code" status.code in
  match code with
  | `success -> ()
  | _ -> failwith "todo: other status"



let verify_cluster_version (header:Command_header.t) my_cluster_version =

  let cluster_version =
    unwrap_option "header.connection_id" header.cluster_version
  in
  let () = Printf.printf "cluster_version: %Lx\n%!" cluster_version in
  assert (my_cluster_version = cluster_version);
  ()

let verify_limits log = ()

open Lwt

let maybe_read_value ic value_ln =
  match value_ln with
  | 0 -> Lwt.return None
  | n -> let vb = Bytes.create value_ln in
         Lwt_io.read_into_exactly ic vb 0 value_ln >>= fun () ->
         Lwt.return (Some vb)

let network_receive ic =
  let msg_bytes = Bytes.create 9 in
  Lwt_io.read_into_exactly ic msg_bytes 0 9 >>= fun () ->
  let magic = msg_bytes.[0] in
  let proto_ln = _decode_fixed32 msg_bytes 1 in
  let value_ln = _decode_fixed32 msg_bytes 5 in
  (*
    Lwt_io.printlf
    "magic:%C proto_ln:%i value_ln:%i" magic proto_ln value_ln
    >>= fun () ->
   *)
  assert (magic = 'F');
  let proto_raw = Bytes.create proto_ln in
  Lwt_io.read_into_exactly ic proto_raw 0 proto_ln >>= fun () ->
  (*Lwt_io.printlf "proto_raw:\n%s" (to_hex proto_raw) >>= fun () -> *)

  maybe_read_value ic value_ln >>= fun vo ->
  let buf = Piqirun.init_from_string proto_raw in
  let m = parse_message buf in
  Lwt.return (m,vo)


let network_send oc proto_raw vo =
  let prelude = Bytes.create 9 in
  Bytes.set prelude 0 'F';
  (*Lwt_log.debug_f "proto_raw:%s\n" (to_hex proto_raw) >>= fun () -> *)
  _encode_fixed32 prelude 1 (String.length proto_raw);
  let write_vo = match vo with
    |None ->
      begin
        _encode_fixed32 prelude 5 0;
        fun () -> Lwt.return ()
      end
    |Some v ->
      begin
        _encode_fixed32 prelude 5 (Bytes.length v);
        fun () -> Lwt_io.write oc v
      end
  in
  (*Lwt_log.debug_f "prelude:%s\n" (to_hex prelude) >>= fun () ->*)
  Lwt_io.write oc prelude >>= fun () ->
  Lwt_io.write oc proto_raw >>= fun ()->
  write_vo ()

(*

let get_status_detailed_message (m:Message.t) =
  let command = unwrap_option "command" m.command in
  let status  = unwrap_option "status" command.status in
  let dm      = unwrap_option "detailed" status.detailed_message
  in
  dm

 *)

module Kinetic = struct
    type session = {
        secret: string;
        cluster_version: int64;
        identity: int64;
        connection_id: int64;
        mutable sequence: int64;
      }

    type key = bytes
    type value = bytes
    type connection = Lwt_io.input_channel * Lwt_io.output_channel
    let incr_sequence t = t.sequence <- Int64.succ t.sequence

    let _parse_command (m:Message.t) =
      let open Message in
      let command_bytes = unwrap_option "command_bytes" m.command_bytes in
      let command_buf = Piqirun.init_from_string command_bytes in
      let command = parse_command command_buf in
      command

    let handshake secret cluster_version (ic,oc) =
      network_receive ic >>= fun (m,vo) ->
      let () = maybe_verify_msg m in
      let command = _parse_command m in
      let status = unwrap_option "command.status" command.status in
      let () = verify_status status in
      let header = unwrap_option "command.header" command.header in
      let open Command_header in
      let connection_id = unwrap_option "header.connection_id" header.connection_id in
      Lwt_io.printlf "connection_id:%Li" connection_id >>= fun () ->
      let () = verify_cluster_version header cluster_version in
      let open Command_body in
      let body = unwrap_option "command.body" command.body in

      let log = unwrap_option "body.get_log" body.get_log in
      (*
         self.config = cmd.body.getLog.configuration
         self.limits = cmd.body.getLog.limits
       *)

      let () = verify_limits log in
      (*

       *)
      let session ={
          cluster_version;
          identity = 1L;
          sequence = 0L;
          secret;
          connection_id
        }
      in
      Lwt.return session



  let make_serialized_msg session mt body_manip =
    let open Message_hmacauth in
    let command = default_command () in
    let header = default_command_header () in
    let () = header.cluster_version <- Some session.cluster_version in
    let () = header.connection_id <- Some session.connection_id in
    let () = header.ack_sequence <- Some session.sequence in
    let () = command.header <- Some header in

    let body = default_command_body () in
    let () = body_manip body in
    let () = command.body <- Some body in
    let m = default_message () in

    let () = header.message_type <- Some mt in


    let command_bytes = Piqirun.to_string(gen_command command) in
    m.command_bytes <- Some command_bytes;
    let hmac = calculate_hmac session.secret command_bytes in
    let hmac_auth = default_message_hmacauth() in
    hmac_auth.identity <- Some session.identity;
    hmac_auth.hmac <- Some hmac;
    m.hmac_auth <- Some hmac_auth;
    let proto_raw = Piqirun.to_string(gen_message m) in
    proto_raw


  let set_ko ko (body:Command_body.t) =
    let open Command_key_value in
    let kv = default_command_key_value() in
    let () = kv.key <- ko in
    let () = kv.force <- Some true in
    let () = body.key_value <- Some kv in
    ()

  let make_set session key    =
      make_serialized_msg session `put  (set_ko (Some key))

  let make_delete session key =
      make_serialized_msg session `delete (set_ko (Some key))

  let make_start_batch session batch_id =
    let open Message_hmacauth in
    let command = default_command () in
    let header = default_command_header () in
    header.cluster_version <- Some session.cluster_version;
    header.connection_id <- Some session.connection_id;
    header.ack_sequence <- Some session.sequence;
    header.batch_id <- Some batch_id;

    let () = command.header <- Some header in
    let body = default_command_body () in

    (* no body manip *)

    let () = command.body <- Some body in
    let m = default_message () in

    let () = header.message_type <- Some `start_batch in


    let command_bytes = Piqirun.to_string(gen_command command) in
    m.command_bytes <- Some command_bytes;
    let hmac = calculate_hmac session.secret command_bytes in
    let hmac_auth = default_message_hmacauth() in
    hmac_auth.identity <- Some session.identity;
    hmac_auth.hmac <- Some hmac;
    m.hmac_auth <- Some hmac_auth;
    let proto_raw = Piqirun.to_string(gen_message m) in
    proto_raw


  let set session (ic,oc) k vo =
    match vo with
    | None -> (* it's a delete *)
       begin
         let msg = make_delete session k in
         network_send oc msg None >>= fun () ->
         network_receive ic >>= fun (r, vo) ->
         assert (vo = None);
         let command = _parse_command r in
         let () = incr_sequence session in
         _assert_type command `delete_response;
         _assert_success command;
         Lwt.return ()
       end
    | Some _ ->
       begin
         let msg = make_set session k in
         network_send oc msg vo >>= fun ()->
         network_receive ic >>= fun (r, vo) ->
         assert (vo = None);
         let command = _parse_command r in
         _assert_type command `put_response;
         _assert_success command;
         let () = incr_sequence session in
         Lwt.return ()
       end

  let make_get session key =
      make_serialized_msg session `get  (set_ko (Some key))

  let get session (ic,oc) k =
    let msg = make_get session k in
    network_send oc msg None >>= fun () ->
    network_receive ic >>= fun (r,vo) ->
    let command = _parse_command r in
    _assert_type command  `get_response;
    let status = _get_status command in
    let code = _get_status_code status in
    let () = incr_sequence session in
    match code with
    | `not_found -> Lwt.return None
    | `success    -> Lwt.return vo
    | x ->
       let (int_code:int) = Obj.magic x in
       Lwt_io.printlf "x=%i\n%!" int_code >>= fun () ->
       let sm = _get_status_message status in
       Lwt.fail (Failure sm)

  let set_kr start_key sinc end_key einc reverse_results max_results body =
    let open Command_range in
    let range = default_command_range () in
    range.start_key <- Some start_key;
    range.start_key_inclusive <- Some sinc;
    range.end_key   <- Some end_key;
    range.end_key_inclusive <- Some einc;
    range.reverse <- Some reverse_results;
    let max32 = Int32.of_int max_results in
    range.max_returned <- Some max32;
    body.range <- Some range;
    ()

  let make_get_key_range session
                         start_key sinc
                         end_key einc
                         reverse_results
                         max_results =
      let manip = set_kr
                    start_key sinc
                    end_key einc
                    reverse_results
                    max_results
      in
      make_serialized_msg session `getkeyrange manip

  let get_key_range_result (command : Command.t) =
    try (* it's not me, it's the server *)
      let body    = unwrap_option "body" command.body in
      let range   = unwrap_option "range" body.range in
      let open Command_range in
      let (keys:string list) = range.keys in
      keys
    with Failure _ -> ([]:string list)


  let get_key_range session (ic,oc)
                    (start_key:string) sinc
                    (end_key:string) einc
                    (reverse_results:bool)
                    (max_results:int) =
    let msg = make_get_key_range session start_key sinc
                                 end_key einc reverse_results
                                 max_results
    in
    network_send oc msg None >>= fun () ->
    network_receive ic       >>= fun (r,vo) ->
    let command = _parse_command r in
    _assert_type command `getkeyrange_response;
    let () = incr_sequence session in
    let status = _get_status command in
    let code = _get_status_code status in
    match code with
    | `success ->
       let key_list = get_key_range_result command in
       Lwt.return key_list
    | _ -> let sm = _get_status_message status in
           Lwt.fail (Failure sm)


  let start_batch_operation session (ic,oc) batch_id =
    let msg = make_start_batch session batch_id in
    network_send oc msg None >>= fun () ->
    network_receive ic >>= fun (r,vo) ->
    Lwt.return (r,vo)


(*

    let make_noop session =
      make_serialized_msg session `noop (set_ko None)
    let make_set session key    =
      make_serialized_msg session `put  (set_ko (Some key))
    let make_delete session key =
      make_serialized_msg session `delete (set_ko (Some key))





    let make_p2p_push session (host,port,tls) operations =
      let open Message_p2_poperation in
      let open Message_p2_poperation_operation in
      let open Message_p2_poperation_peer in
      let manip body =
        let poperations =
          List.map
          (fun ((k:string),(nko:string option)) ->
           let pop = default_message_p2_poperation_operation () in
           pop.key <- Some k;
           pop.new_key <- nko;
           pop
          ) operations
        in
        let ppeer = default_message_p2_poperation_peer () in
        let () = ppeer.hostname <- Some host in
        let () = ppeer.port <- Some (Int32.of_int port) in
        let () = ppeer.tls <- Some tls in

        let p2pop = default_message_p2_poperation () in
        let () = p2pop.peer <- Some ppeer in
        let () = p2pop.operation <- poperations in
        let () = body.p2p_operation <- Some p2pop in
        ()
      in
      make_serialized_msg session `peer2_peerpush manip



    let noop session (ic,oc) =
      let msg = make_noop session in
      let vo = None in
      network_send oc msg vo >>= fun () ->
      Lwt_log.debug "done sending" >>= fun () ->
      network_receive ic >>= fun (r,vo) ->
      assert (vo = None);
      _assert_type r `noop_response;
      _assert_success r;

      let () = incr_session session in
      Lwt.return ()








    let p2p_push session (ic,oc) peer operations =
      let msg = make_p2p_push session peer operations in
      network_send oc msg None >>= fun () ->
      network_receive ic >>= fun (r,vo) ->
      _assert_type r `peer2_peerpush_response;
      let () = incr_session session in
      let status = get_status_code r in
      let lwt_fail x = Lwt.fail(Failure x) in
      match status with
      | `success -> Lwt.return ()
      | `invalid_status_code
      | `not_attempted
      | `hmac_failure
      | `not_authorized
      | `version_failure
      | `internal_error
      | `header_required
      | `not_found -> lwt_fail "not_found"
      | `version_mismatch -> lwt_fail "version_mismatch"
      | `service_busy -> lwt_fail "service_busy"
      | `expired -> lwt_fail "expired"
      | `data_error -> lwt_fail "data_error"
      | `perm_data_error -> lwt_fail "perm_data_error"
      | `remote_connection_error ->
         lwt_fail ("remote_connection_error: ")
      | `no_space -> lwt_fail "no_space"
      | `no_such_hmac_algorithm -> lwt_fail "no_such_hmac_algorithm"
      | `invalid_request ->
         let (int_status:int) = Obj.magic status in
         Lwt_io.printlf "x=%i\n%!" int_status >>= fun () ->
         let sm = get_status_message r in
         Lwt.fail (Failure sm)

 *)
  end
