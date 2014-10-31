open Kinetic_piqi
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

open Lwt

let network_receive ic =
  let msg_bytes = Bytes.create 9 in
  Lwt_io.read_into_exactly ic msg_bytes 0 9 >>= fun () ->
  (*let magic = msg_bytes.[0] in*)
  let proto_ln = _decode_fixed32 msg_bytes 1 in
  let value_ln = _decode_fixed32 msg_bytes 5 in
  (*
  Lwt_log.debug_f
      "magic:%C proto_ln:%i value_ln:%i" magic proto_ln value_ln
  >>= fun () ->
  *)
  let proto_raw = Bytes.create proto_ln in
  Lwt_io.read_into_exactly ic proto_raw 0 proto_ln >>= fun () ->
  (*Lwt_log.debug_f "proto_raw:%s" (to_hex proto_raw) >>= fun () ->*)
  let maybe_read_value ic =
    match value_ln with
    | 0 -> Lwt.return None
    | n -> let vb = Bytes.create value_ln in
           Lwt_io.read_into_exactly ic vb 0 value_ln >>= fun () ->
           Lwt.return (Some vb)
  in
  maybe_read_value ic >>= fun vo ->
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


let unwrap_option msg = function
  | None -> failwith ("None " ^ msg)
  | Some x -> x

open Message
open Message_command
open Message_header
open Message_status
open Message_body

let get_status_code (m:Message.t) :message_status_status_code =
  let command = unwrap_option "command" m.command in
  let status = unwrap_option "status" command.status  in
  let code = unwrap_option "code" status.code in
  code

let get_status_message (m:Message.t) =
  let command = unwrap_option "command" m.command in
  let status  = unwrap_option "status" command.status in
  let sm      = unwrap_option "status_message" status.status_message in
  sm

let get_status_detailed_message (m:Message.t) =
  let command = unwrap_option "command" m.command in
  let status  = unwrap_option "status" command.status in
  let dm      = unwrap_option "detailed" status.detailed_message
  in
  dm

let get_key_range_result (m:Message.t) =
  let open Message_range in
  let command = unwrap_option "command" m.command in
  try (* it's not me, it's the server *)
    let body    = unwrap_option "body" command.body in
    let range   = unwrap_option "range" body.range in
    range.key
  with Failure _ -> ([]:string list)

let _assert_success r =
  let status = get_status_code r in
  assert (status = `success)

let _assert_type (r:Message.t) typ =
  let open Message in
  let command = unwrap_option "command "r.command in
  let header  = unwrap_option "header" command.header in
  let htyp    = unwrap_option "message type" header.message_type in
  assert (htyp = typ)

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

    let make_session ?(cluster_version = 0L)
                     ?(identity = 1L)
                     ?(sequence = 0L)
                     ~secret
                     ~connection_id
      =
      {
        secret;
        cluster_version;
        identity;
        connection_id;
        sequence;
      }

    let set_ko ko body =
      let open Message_key_value in
      let kv = default_message_key_value() in
      let () = kv.key <- ko in
      let () = body.key_value <- Some kv in
      ()

    let set_kr start_key sinc end_key einc reverse_results max_results body =
      let open Message_range in
      let range = default_message_range () in
      let () = range.start_key <- Some start_key in
      let () = range.start_key_inclusive <- Some sinc in
      let () = range.end_key   <- Some end_key in
      let () = range.end_key_inclusive <- Some einc in
      let () = range.reverse <- Some reverse_results in
      let max32 = Int32.of_int max_results in
      let () = range.max_returned <- Some max32 in
      let () = body.range <- Some range in
      ()

    let make_serialized_msg session mt body_manip =
      let command = default_message_command () in
      let header = default_message_header () in
      let () = header.cluster_version <- Some session.cluster_version in

      let () = header.identity <- Some session.identity in
      let () = header.connection_id <- Some session.connection_id in
      let () = header.sequence <- Some session.sequence in
      let () = command.header <- Some header in
      let body = default_message_body () in
      let () = body_manip body in
      let () = command.body <- Some body in
      let m = default_message () in
      m.command <- Some command;
      let () = header.message_type <- Some mt in
      let command_raw = Piqirun.to_string(gen_message_command command) in
      let hmac = calculate_hmac session.secret command_raw in
      m.hmac <- Some hmac;
      let proto_raw = Piqirun.to_string(gen_message m) in
      proto_raw


    let make_noop session =
      make_serialized_msg session `noop (set_ko None)
    let make_set session key    =
      make_serialized_msg session `put  (set_ko (Some key))
    let make_delete session key =
      make_serialized_msg session `delete (set_ko (Some key))
    let make_get    session key =
      make_serialized_msg session `get  (set_ko (Some key))

    let make_get_key_range session
                           start_key sinc
                           end_key einc
                           reverse_results
                           max_results =
      let manip = set_kr
                    start_key sinc end_key einc reverse_results
                    max_results
      in
      make_serialized_msg session `getkeyrange manip


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

    let incr_session t = t.sequence <- Int64.succ t.sequence

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

    let set session (ic,oc) k vo =
      match vo with
      | None -> (* it's a delete *)
         begin
           let msg = make_delete session k in
           network_send oc msg None >>= fun () ->
           network_receive ic >>= fun (r,vo) ->
           let () = incr_session session in
           _assert_type r `delete_response;
           _assert_success r;
           Lwt.return ()
         end
      | Some _ ->
         begin
           let msg = make_set session k in
           network_send oc msg vo >>= fun ()->
           network_receive ic >>= fun (r,vo) ->
           assert (vo = None);
           _assert_type r `put_response;
           _assert_success r;
           let () = incr_session session in
           Lwt.return ()
         end

    let get session (ic,oc) k =
      let msg = make_get session k in
      network_send oc msg None >>= fun () ->
      network_receive ic >>= fun (r,vo) ->
      _assert_type r `get_response;
      let status = get_status_code r in
      let () = incr_session session in
      match status with
      | `not_found -> Lwt.return None
      | `success    -> Lwt.return vo
      | x ->
         let (int_code:int) = Obj.magic x in
         Lwt_io.printlf "x=%i\n%!" int_code >>= fun () ->
         let sm = get_status_message r in
             Lwt.fail (Failure sm)


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
      network_receive ic >>= fun (r,vo) ->
      _assert_type r `getkeyrange_response;
      let () = incr_session session in
      let status = get_status_code r in
      match status with
      | `success ->
         let key_list = get_key_range_result r in
         Lwt.return key_list
      | _ -> let sm = get_status_message r in
             Lwt.fail (Failure sm)

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
  end
