(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open Cryptokit
open Kinetic_util
open Kinetic_network

let calculate_hmac secret msg =
  let sx0 = Bytes.create 4 in
  _encode_fixed32 sx0 0 (String.length msg);
  let h = Cryptokit.MAC.hmac_sha1 secret in
  let () = h # add_string sx0 in
  let () = h # add_string msg in
  h # result


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

let _get_detailed_status_message (status:Command_status.t) = get_option "None" status.detailed_message

let status_code2i = function
  | `invalid_status_code     -> -1
  | `not_attempted           ->  0 (* p2p *)
  | `success                 ->  1
  | `hmac_failure            ->  2
  | `not_authorized          ->  3
  | `version_failure         ->  4
  | `internal_error          ->  5
  | `header_required         ->  6
  | `not_found               ->  7
  | `version_mismatch        ->  8
  | `service_busy            ->  9
  | `expired                 -> 10
  | `data_error              -> 11
  | `perm_data_error         -> 12
  | `remote_connection_error -> 13
  | `no_space                -> 14
  | `no_such_hmac_algorithm  -> 15
  | `invalid_request         -> 16
  | `nested_operation_errors -> 17
  | `device_locked           -> 18
  | `device_already_unlocked -> 19
  | `connection_terminated   -> 20
  | `invalid_batch           -> 21 (* 3.0.6 *)
  | _ -> 42

let _parse_command (m:Message.t) =
  let open Message in
  let command_bytes = unwrap_option "command_bytes" m.command_bytes in
  let command_buf = Piqirun.init_from_string command_bytes in
  let command = parse_command command_buf in
  command

module Error = Kinetic_error.Error

let _assert_success (command:Command.t) =
  let code = _get_status command |> _get_status_code in
  assert (code = `success)

let message_type2s = function
  | `invalid_message_type -> "invalide_message_type"
  | `get -> "get"
  | `get_response -> "get_response"
  | `put -> "put"
  | `put_response -> "put_response"
  | `delete -> "delete"
  | `delete_response -> "delete_response"
  | `start_batch_response -> "start_batch_response"
  | `end_batch_response -> "end_batch_response"
  | `flushalldata -> "flushalldata"
  | `flushalldata_response -> "flushalldata_response"
  | `getlog -> "getlog"
  | `getlog_response -> "getlog_response"
  | _ -> "TODO: message type2s"

let auth_type2s = function
   | `invalid_auth_type -> "invalid_auth_type"
   | `hmacauth -> "hmacauth"
   | `pinauth -> "pinauth"
   | `unsolicitedstatus -> "unsolicitedstatus"

let status_code2s = function
  | `invalid_status_code -> "invalid_status_code"
  | `not_attempted -> "not_attempted"
  | `success -> "success"
  | `hmac_failure -> "hmac_failure"
  | `not_authorized -> "not_authorized"
  | `version_failure -> "version_failure"
  | `internal_error -> "internal_error"
  | `header_required -> "header_required"
  | `not_found -> "not_found"
  | `version_mismatch -> "version_mismatch"
  | `service_busy -> "service_busy"
  | `expired -> "expired"
  | `data_error -> "data_error"
  | `perm_data_error -> "perm_data_error"
  | `remote_connection_error ->"remove_connection_error"
  | `no_space -> "no_space"
  | `no_such_hmac_algorithm -> "no_such_hmac_algorithm"
  | `invalid_request -> "invalid_request"
  | _ -> "TODO: status_code2s"



let _get_message_type (command:Command.t) =
  let header = unwrap_option "header" command.header in
  unwrap_option "message_type" header.message_type


let _get_message_auth_type msg = unwrap_option "auth_type" msg.auth_type

let _assert_both m command typ code =
  let auth_type = _get_message_auth_type m in
  match auth_type with
  | `hmacauth | `pinauth ->
     begin
       let header = unwrap_option "header" command.header in
       let htyp = unwrap_option "message type" header.message_type in
       if htyp = typ
       then
         begin
           let status = _get_status command in
           let ccode = _get_status_code status in
           if ccode = code
           then Lwt_result.return ()
           else
             begin
               let () = Printf.printf "ccode:%s\n%!" (status_code2s ccode) in
               let sm = _get_status_message status in
               let rci = status_code2i ccode in
               Lwt_result.fail (Error.KineticError(rci, sm))
             end
         end
       else
         let msg = htyp |>
               message_type2s |>
                     Printf.sprintf "unexpected type: %s"
         in
         let e = Error.Generic(__FILE__,__LINE__, msg) in
         Lwt_result.fail e
     end
  | `unsolicitedstatus ->
     let status = _get_status command in
     let ccode = _get_status_code status in
     let sm = _get_status_message status in
     let rci = status_code2i ccode in
     Lwt_result.fail (Error.KineticError(rci, sm))
  | `invalid_auth_type ->
     let e = Error.Assert "invalid_auth_type" in
     Lwt_result.fail e



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

  let cluster_version = get_option 0L header.cluster_version in
  assert (my_cluster_version = cluster_version);
  ()

let verify_limits log = ()

open Lwt




let _get_sequence (command : Command.t) =
  let header = unwrap_option "header" command.header in
  let seq = unwrap_option "sequence" header.sequence in
  seq

let _get_ack_sequence (command:Command.t) =
  let header = unwrap_option "header" command.header in
  let ack_seq = unwrap_option "ack_sequence" header.ack_sequence in
  ack_seq


module Config = Kinetic_config.Config
module Session = Kinetic_session.Session
module Tag = Kinetic_tag.Tag

include Kinetic_integration

module Batch(I:INTEGRATION) =
struct

  type rc = | Ok | Nok of int * bytes

  type handler = rc -> unit Lwt.t

  type t = { mvar :  bool Lwt_mvar.t;
             handlers : (command_message_type,
                         rc -> unit Lwt.t) Hashtbl.t;
             socket : I.socket;
             batch_id : int32;
             go : bool ref;
             session : Session.t;
             mutable count : int;
             mutable error : Error.t option;
           }

  let find t h =
    try Some (Hashtbl.find t h)
    with Not_found -> None

  let failed t e =
    let () = match t.error with
    | None -> t.error <- Some e
    | Some _ -> ()
    in
    t.go := false;
    Lwt_mvar.put t.mvar false

  let remove t h = Hashtbl.remove t h

  let make session (socket:I.socket) batch_id =
    let timeout =
      let cfg = session.Session.config in
      cfg.timeout
    in
    let handlers = Hashtbl.create 5 in
    let mvar = Lwt_mvar.create_empty () in
    let go = ref true in

    let batch =
      { mvar  ; handlers ; socket; batch_id; go = go;
        session; count = 0; error = None }
    in
    let success = ref true in
    let rec loop (go:bool ref) (socket:I.socket) =
      let size = Hashtbl.length handlers in
      if size > 0 || !go
      then
        begin
          Lwt_log.debug ~section "waiting for msg" >>= fun () ->
          network_receive_generic
            ~timeout I.create I.read I.read_bytes socket I.show_socket session.Session.trace
          >>=? fun (m,vo, proto_raw) ->
          Lwt_log.debug ~section "got msg" >>= fun () ->
          let auth_type = _get_message_auth_type (m:Message.t) in
          let command = _parse_command m in
          begin
            match auth_type with
            | `hmacauth ->
               begin
                 let typ =
                   try _get_message_type command
                   with
                   | exn ->
                      let () = Lwt_log.ign_info_f "no type for: %s" (to_hex proto_raw) in
                      raise exn
                 in
                 let typs = message_type2s typ in
                 begin
                   match find handlers typ with
                   | None ->
                      Lwt_log.info_f ~section "\tignoring: %s" typs
                   | Some h ->
                      Lwt_log.debug_f ~section "found handler for: %s" typs
                      >>= fun () ->
                      let () = remove handlers typ in
                      let status = _get_status command in
                      let ccode = _get_status_code status in
                      let rc =
                        match ccode with
                        | `success -> Ok
                        | _ ->
                           let sm = _get_status_message status in
                           let dsm = _get_detailed_status_message status in
                           Lwt_log.ign_debug_f ~section
                             "dsm:%S" dsm;
                           let (rci:int) = status_code2i ccode in
                           Nok (rci, sm)
                      in
                      Lwt.catch
                        (fun () ->h rc)
                        (fun exn -> go:= false;
                                    success := false;
                                    Lwt.return_unit)
                 end
               end
            | `unsolicitedstatus ->
               begin
                 let () = Lwt_log.ign_info_f "unsolicitedstatus: %s" (to_hex proto_raw) in
                 let status = _get_status command in
                 let ccode = _get_status_code status in
                 let sm = _get_status_message status in
                 let rci = status_code2i ccode in
                 batch.error <- Some (KineticError(rci,sm)) ;
                 go := false;
                 Lwt.return_unit
               end
            | `pinauth | `invalid_auth_type -> assert false
          end
          >>= fun ()->
          loop go socket
        end
      else
        begin
          Lwt_mvar.put mvar !success >>= fun () ->
          Lwt_log.debug_f ~section "loop ends here (success:%b)" !success >>= fun () ->
          Lwt_result.return ()
        end
    in
    let t =
      loop go socket
      >>= function
      | Result.Ok ()   -> Lwt.return_unit
      | Result.Error e ->
         Lwt_log.debug_f ~section "batch loop for %li failed:%s" batch_id (Error.show e)
         >>= fun ()->
         let rci = status_code2i `internal_error in
         let rc_bad = Nok (rci, Error.show e) in
         Hashtbl.iter (fun k h ->
                       Lwt.ignore_result (h rc_bad);
                      ) handlers;
         Lwt.return_unit
    in
    let () = Lwt.ignore_result t
    in batch


  let inc_count t = t.count <- t.count + 1

  let add_handler t typ h =
    let typs = message_type2s typ in
    Lwt_log.debug_f ~section "add handler for: %s" typs >>= fun () ->
    Hashtbl.add t.handlers typ h;
    Lwt.return_unit

  let close t =
    Lwt_log.debug ~section "closing batch" >>= fun () ->
    t.go := false;
    Lwt_mvar.take  t.mvar
    >>= function
    | true  -> Lwt_result.return ()
    | false ->
       begin
         match t.error with
         | None -> assert false (* TODO: This is a sign *)
         | Some e ->
            Lwt_log.debug_f "closing with error: %s" (Error.show e) >>= fun () ->
            Lwt_result.fail e
       end
end

  
module Make(I:INTEGRATION) = struct

  module Entry = struct
    type t = {
        key:key;
        db_version:version;
        new_version : version;
        vt: (I.value slice * Tag.t) option;
      }

    let make ~key ~db_version ~new_version vt = { key; db_version; new_version; vt }


    let show e =
      let vt2s = show_option
                   (show_pair
                      (show_tuple3 I.show string_of_int string_of_int)
                      Tag.show)
      in
      Printf.sprintf "{ key=%S; db_version=%s; new_version=%s; vo=%s }"
        e.key
        (so2hs e.db_version)
        (so2hs e.new_version)
        (vt2s e.vt)
  end



  type 'a result = ('a, Error.t) Lwt_result.t
  module B = Batch(I)

    type session = Session.t

    let get_connection_id (session:session) =
      let open Session in session.connection_id


    type batch = B.t
    let get_batch_id (batch:batch) =
      let open B in batch.batch_id
    type synchronization =
      |WRITETHROUGH
      |WRITEBACK
      |FLUSH
    

    type rc = B.rc

    let convert_rc = function
      | B.Ok -> None
      | B.Nok(i,m) -> Some (i,m)



    type handler = B.handler

    type version = bytes option

    type closer = unit -> unit Lwt.t

    type client = {
        session : session ;
        socket: I.socket;
        closer : closer;
        mutable closed : bool;
      }

    let make_sha1  (v_buf, v_off,v_len) = I.make_sha1  v_buf v_off v_len
    let make_crc32 (v_buf, v_off,v_len) = I.make_crc32 v_buf v_off v_len
                         
    let get_config (session:Session.t) =
      let open Session in
      session.config

    let handshake secret cluster_version ?(trace = false) ?(timeout=10.0) ?max_operation_count_per_batch socket  =
      network_receive_generic ~timeout I.create I.read I.read_bytes socket I.show_socket trace >>=? fun (m,vo,_) ->
      let () = maybe_verify_msg m in
      let command = _parse_command m in
      let status = unwrap_option "command.status" command.status in
      let () = verify_status status in
      let header = unwrap_option "command.header" command.header in
      let open Command_header in
      let connection_id = unwrap_option "header.connection_id" header.connection_id in
      Lwt_log.debug_f ~section "connection_id:%Li" connection_id >>= fun () ->
      Lwt_log.debug_f "sequence:%s" (i64o2s header.sequence) >>= fun () ->
      Lwt_log.debug_f "ack_sequence:%s" (i64o2s header.ack_sequence) >>= fun () ->
      let () = verify_cluster_version header cluster_version in
      let open Command_body in
      let body = unwrap_option "command.body" command.body in
      let open Command_get_log in
      let log = unwrap_option "body.get_log" body.get_log in
      (*
         self.config = cmd.body.getLog.configuration
         self.limits = cmd.body.getLog.limits
       *)

      let open Command_get_log_configuration in
      let () = verify_limits log in
      let cfg = unwrap_option "configuration" log.configuration in
      let wwn = unwrap_option "world_wide_name" cfg.world_wide_name in
      let interfaces = cfg.interface in
      let ipv4_addresses =
        List.fold_left
          (fun acc interface ->
            let open Command_get_log_configuration_interface in
            let ip4bin = unwrap_option "ipv4_address" interface.ipv4_address in
            if ip4bin = "" (* this nic has no connection, skip it *)
            then acc
            else ip4bin :: acc
          ) []
          interfaces
        |> List.rev
      in
      let vendor = unwrap_option "vendor" cfg.vendor in
      let model = unwrap_option "model" cfg.model in
      let serial_number = unwrap_option
                            "serial_number"
                            cfg.serial_number in
      let version = unwrap_option "version" cfg.version in
      let limits = unwrap_option "limits" log.limits in
      let open Command_get_log_limits in
      let int_of k o =
        let m_k_s32 = unwrap_option k o in
        Int32.to_int m_k_s32
      in
      let max_key_size     = int_of "max_key_size" limits.max_key_size
      and max_value_size   = int_of "max_value_size" limits.max_value_size
      and max_version_size = int_of "max_version_size" limits.max_version_size
      and max_tag_size     = int_of "max_tag_size" limits.max_tag_size
      and max_connections  = int_of "max_connections" limits.max_connections
      and max_outstanding_read_requests =
        int_of "max_outstanding_read_requests" limits.max_outstanding_read_requests
      and max_outstanding_write_requests =
        int_of "max_outstanding_write_requests" limits.max_outstanding_write_requests
      and max_message_size = int_of "max_message_size" limits.max_message_size
      and max_key_range_count =
        int_of "max_key_range_count" limits.max_key_range_count
      (* and max_operation_count_per_batch =
        int_of "max_operation_count_per_batch" limits.max_operation_count_per_batch

      and max_batch_count_per_device =
        int_of "max_batch_count_per_device" limits.max_batch_count_per_device
       *)
      and max_batch_size        = map_option Int32.to_int limits.max_batch_size
      and max_deletes_per_batch = map_option Int32.to_int limits.max_deletes_per_batch
      in
      let max_operation_count_per_batch =
        if String.length version >= 8
           && String.sub version 0 8 = "07.00.03"
        then Some 15
        else max_operation_count_per_batch
      in
      let config =
        Config.make ~vendor
                    ~world_wide_name:wwn
                    ~model
                    ~serial_number
                    ~version
                    ~ipv4_addresses
                    ~max_key_size
                    ~max_value_size
                    ~max_version_size
                    ~max_tag_size
                    ~max_connections
                    ~max_outstanding_read_requests
                    ~max_outstanding_write_requests
                    ~max_message_size
                    ~max_key_range_count
                    ~max_operation_count_per_batch
                    ~max_batch_size
                    ~max_deletes_per_batch
                    ~timeout
                    (* ~max_batch_count_per_device *)
      in
      Lwt_log.debug_f "config=%s" (Config.show config) >>= fun () ->
      let session =
        let open Session in {
          cluster_version;
          identity = 1L;
          sequence = 1L;
          secret;
          connection_id;
          batch_id = 1l;
          config ;
          trace;
          in_batch = false;
        }
      in
      Lwt_result.return session


    let _assert_response (m:Message.t) (client:client) =
      let open Message in
      match m.auth_type with
      | Some `unsolicitedstatus ->
         begin
           let command = _parse_command m in
           let status = _get_status command in
           let ccode = _get_status_code status in
           let sm = _get_status_message status in
           let rci = status_code2i ccode in
           let e = Error.KineticError(rci,sm) in
           client.closed <- true;
           client.closer () >>= fun () ->
           Lwt_result.fail e
         end
      | _ -> Lwt_result.return ()


  let make_serialized_msg ?timeout ?priority session mt body_manip =
    let open Message_hmacauth in
    let open Session in
    let command = default_command () in
    let header = default_command_header () in
    let () = header.cluster_version <- Some session.cluster_version in
    let () = header.connection_id <- Some session.connection_id in
    let () = header.sequence <- Some session.sequence in
    let () = header.timeout <- timeout in
    let () = header.priority <- priority in
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
    m.auth_type <- Some `hmacauth;
    m.hmac_auth <- Some hmac_auth;
    let proto_raw = Piqirun.to_string(gen_message m) in
    proto_raw

  let make_pin_auth_serialized_msg session (pin:string) mt body_manip =

    let open Session in
    let command = default_command () in
    let header = default_command_header () in
    let () = header.cluster_version <- Some session.cluster_version in
    let () = header.connection_id <- Some session.connection_id in
    let () = header.sequence <- Some session.sequence in

    let () = command.header <- Some header in

    let body = default_command_body () in
    let () = body_manip body in
    let () = command.body <- Some body in
    let m = default_message () in

    let () = header.message_type <- Some mt in


    let command_bytes = Piqirun.to_string(gen_command command) in
    m.command_bytes <- Some command_bytes;

    m.auth_type <- Some `pinauth;
    let pin_auth = default_message_pinauth() in
    let () = pin_auth.Message_pinauth.pin <- Some pin in
    m.pin_auth <- Some pin_auth;
    let proto_raw = Piqirun.to_string(gen_message m) in
    proto_raw

  let set_attributes ~ko
                     ~db_version
                     ~new_version
                     ~forced
                     ~synchronization
                     ~maybe_tag
                     (body:Command_body.t)
    =
    let open Command_key_value in
    let kv = default_command_key_value() in
    kv.key <- ko;
    kv.force <- forced;
    kv.db_version <- db_version;
    kv.new_version <- new_version;
    let () = match maybe_tag with
    | None -> ()
    | Some tag ->
       begin
         match tag with
         | Tag.Invalid h ->
            begin
              kv.algorithm <- Some `invalid_algorithm;
              kv.tag <- Some h;
            end
         | Tag.Sha1 h ->
            begin
              kv.algorithm <- Some `sha1;
              kv.tag <- Some h
            end
         | Tag.Crc32 h ->
            let s = Bytes.create 4 in
            _encode_fixed32 s 0 (Int32.to_int h);
            kv.algorithm <- Some `crc32;
            kv.tag <- Some s
       end
    in
    body.key_value <- Some kv;
    let translate = function
      | WRITETHROUGH -> `writethrough
      | WRITEBACK -> `writeback
      | FLUSH     -> `flush
    in
    let sync = map_option translate synchronization in
    kv.synchronization <- sync;
    ()

  let make_delete_forced ?timeout ?priority client key =
    let mb = set_attributes ~ko:(Some key)
                            ~db_version:None
                            ~new_version:None
                            ~forced:(Some true)
                            ~maybe_tag:None
                            ~synchronization:(Some WRITEBACK)
    in
    make_serialized_msg ?timeout ?priority client.session `delete mb

  let make_put
        ?timeout ?priority client
        key value
        ~db_version ~new_version
        ~forced ~synchronization
        ~tag
    =
    let mb =
      set_attributes
        ~ko:(Some key)
        ~db_version
        ~new_version
        ~forced
        ~synchronization
        ~maybe_tag:tag
    in
    make_serialized_msg ?timeout ?priority client.session `put mb

  let _no_manip _ = ()

  let make_flush ?timeout ?priority session =
    make_serialized_msg ?timeout ?priority session `flushalldata _no_manip

  let make_batch_message
        ?timeout
        ?priority
        ?(body_manip = _no_manip)
        session mt batch_id =
    let open Message_hmacauth in
    let open Session in
    let command = default_command () in
    let header = default_command_header () in
    header.cluster_version <- Some session.cluster_version;
    header.connection_id <- Some session.connection_id;
    header.sequence <- Some session.sequence;
    header.batch_id <- Some batch_id;
    header.timeout <- timeout;
    header.priority <- priority;
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

    m.auth_type <- Some `hmacauth;
    m.hmac_auth <- Some hmac_auth;
    let proto_raw = Piqirun.to_string(gen_message m) in
    proto_raw

  let make_start_batch ?timeout ?priority session batch_id =
    make_batch_message ?timeout ?priority session `start_batch batch_id

  let make_end_batch (b:B.t) =
    let open B in
    let body_manip body =
      let batch = default_command_batch () in
      let count = b.count in
      let open Command_batch in
      batch.count <- Some (Int32.of_int count);
      body.batch <- Some batch
    in
    make_batch_message b.session `end_batch b.batch_id ~body_manip

  let make_abort_batch session batch_id =
    make_batch_message session `abort_batch batch_id

  let tracing (session:Session.t) t =
    session.Session.trace <- t

  let _call client msg vo =
    let socket = client.socket in
    let session = client.session in
    let config = session.config in
    let trace = client.session.trace in
    let timeout = config.timeout in
    network_send_generic             I.write I.write_bytes socket msg vo I.show_socket trace >>= fun () ->
    network_receive_generic I.create I.read  I.read_bytes  socket        I.show_socket trace ~timeout

  let get_session t  = t.session

  let _assert_value_size session value_size =
    let cfg = get_config session in
    let max_value_size = cfg.Config.max_value_size in
    if value_size > max_value_size
    then Lwt.fail_with (Printf.sprintf "value_size:%i > max_value_size:%i" value_size max_value_size)
    else Lwt.return_unit

  let _assert_open (client:client) =
    if client.closed
    then Lwt.fail_with (Printf.sprintf "Generic(%S,%i,%S)" __FILE__ __LINE__ "client is closed")
    else Lwt.return_unit

  let _assert_no_batch (client:client) =
    if client.session.in_batch
    then Lwt.fail_with (Printf.sprintf "Generic(%S,%i,%S)" __FILE__ __LINE__ "client has open batch")
    else Lwt.return_unit

  let put
        ?timeout
        ?priority
        (client:client) k
        ((v_buf, v_off, v_len) as v_slice)
        ~db_version ~new_version
        ~forced
        ~synchronization
        ~tag
    =
    _assert_open client >>= fun () ->
    _assert_no_batch client >>= fun () ->
    _assert_value_size client.session v_len >>= fun () ->

    let msg =
      make_put
        ?timeout ?priority client
        k v_slice
        ~db_version ~new_version
        ~forced ~synchronization
        ~tag
    in
    _call client msg (Some v_slice) >>=? fun (r,vo,_) ->
    assert (vo = None);
    _assert_response r client >>=? fun () ->
    let command = _parse_command r in
    let () = Session.incr_sequence client.session in
    _assert_both r command `put_response `success

  let delete_forced ?timeout ?priority (client:client) k =
    _assert_open client >>= fun () ->
    _assert_no_batch client >>= fun () ->
    let msg = make_delete_forced ?timeout ?priority client k in
    _call client msg None >>=? fun (r, vo, _ ) ->
    _assert_response r client >>=? fun () ->
    assert (vo = None);
    let command = _parse_command r in
    let () = Session.incr_sequence client.session in
    _assert_type command `delete_response;
    _assert_success command;
    Lwt_result.return ()



  let make_get ?timeout ?priority session key =
    let mb = set_attributes ~ko:(Some key)
                            ~db_version:None
                            ~new_version:None
                            ~forced:None
                            ~synchronization:None
                            ~maybe_tag:None
    in
    make_serialized_msg ?timeout ?priority session `get mb

  let get ?timeout ?priority client k =
    _assert_open client >>= fun () ->
    _assert_no_batch client >>= fun () ->
    let msg = make_get ?timeout ?priority client.session k in
    _call client msg None >>=? fun (r,vo, proto_raw) ->
    _assert_response r client >>=? fun () ->
    let command = _parse_command r in

    (* _assert_type command  `get_response;*)

    let status = _get_status command in

    let code = _get_status_code status in
    Lwt_log.debug_f ~section "code=%i" (status_code2i code) >>= fun () ->
    let () = Session.incr_sequence client.session in
    match code with
    | `not_found ->
       Lwt_log.debug_f "`not_found" >>= fun () ->
       Lwt_result.return None
    | `success    ->
       Lwt_log.debug_f "`success" >>= fun () ->
       begin
         let version =
           let body = unwrap_option "body" command.body in
           let open Command_key_value in
           let kv = unwrap_option "kv" body.key_value in
           let db_version = kv.db_version in
           db_version
         in
         let v = match vo with
           | None -> I.create 0
           | Some v -> v
         in
         let result = Some (v, version) in
         Lwt_result.return result
       end
    | x ->
       let code = status_code2i x in
       Lwt_log.info_f ~section "code=%i" code >>= fun () ->
       let sm = _get_status_message status in
       let e = Error.KineticError(code, sm) in
       Lwt_result.fail e

  type log_type =
    | CAPACITIES

  let translate_log_type = function
    | CAPACITIES -> `capacities

  let make_getlog ?timeout ?priority session capacities =
    let mb body =
      let getlog = default_command_get_log () in
      let open Command_get_log in
      getlog.types <- List.map translate_log_type capacities;
      body.get_log <- Some getlog
    in
    make_serialized_msg ?timeout ?priority session `getlog mb

  let get_capacities ?timeout ?priority client =
    _assert_open client >>= fun () ->
    _assert_no_batch client >>= fun () ->
    let msg = make_getlog ?timeout ?priority client.session [CAPACITIES] in
    _call client msg None >>=? fun (m,vo, proto_raw) ->
    _assert_response m client >>=? fun () ->
    let command = _parse_command m in
    _assert_type command `getlog_response;
    let status = _get_status command in
    let code = _get_status_code status in
    let () = Session.incr_sequence client.session in
    match code with
    | `success ->
       let body = unwrap_option "body" command.body in
       let getlog = unwrap_option "getlog" body.get_log in
       let open Command_get_log in
       let capacity = unwrap_option "capacity" getlog.capacity in
       let open Command_get_log_capacity in
       let nominal = unwrap_option "nominal_capacity_in_bytes" capacity.nominal_capacity_in_bytes in
       let portion_full = unwrap_option "portion_full" capacity.portion_full in
       Lwt_result.return (nominal, portion_full)

    | x ->
       let code = status_code2i x in
       let sm = _get_status_message status in
       let e = Error.KineticError(code,sm) in
       Lwt_result.fail e

  let set_kr start_key sinc maybe_end_key reverse_results max_results body =
    let open Command_range in
    let range = default_command_range () in
    range.start_key <- Some start_key;
    range.start_key_inclusive <- Some sinc;

    let () =
      match maybe_end_key with
      | None -> ()
      | Some (end_key, einc) ->
         range.end_key   <- Some end_key;
         range.end_key_inclusive <- Some einc
    in
    range.reverse <- Some reverse_results;
    let max32 = Int32.of_int max_results in
    range.max_returned <- Some max32;
    body.range <- Some range;
    ()

  let make_get_key_range
        ?timeout ?priority session
        start_key sinc
        maybe_end_key
        reverse_results
        max_results =
      let manip = set_kr
                    start_key sinc
                    maybe_end_key
                    reverse_results
                    max_results
      in
      make_serialized_msg ?timeout ?priority session `getkeyrange manip

  let get_key_range_result (command : Command.t) =
    match command.body with
    | None -> []
    | Some body ->
      let range   = unwrap_option "range" body.range in
      let open Command_range in
      let (keys:string list) = range.keys in
      keys


  let get_key_range
        ?timeout ?priority client
        (start_key:string) sinc
        (maybe_end_key: (string * bool) option)
        (reverse_results:bool)
        (max_results:int) =
    _assert_open client >>= fun () ->
    _assert_no_batch client >>= fun () ->
    let msg = make_get_key_range
                ?timeout ?priority client.session
                start_key sinc
                maybe_end_key reverse_results
                max_results
    in
    _call client msg None >>=? fun (r,vo, _) ->
    _assert_response r client >>=? fun () ->
    let command = _parse_command r in
    _assert_type command `getkeyrange_response;
    Session.incr_sequence client.session;
    let status = _get_status command in
    let code = _get_status_code status in
    match code with
    | `success ->
       let key_list = get_key_range_result command in
       Lwt_result.return key_list
    | x -> let sm = _get_status_message status in
           let code = status_code2i x in
           let e = Error.KineticError(code, sm) in
           Lwt_result.fail e

  let default_handler batch rc =
    let open B in
    match rc with
    | Ok        ->
       Lwt_log.debug  ~section "default_handler:Ok"
    | Nok(i,sm) ->
       Lwt_log.info_f ~section "default_handler:NOK!: rc:%i; sm=%S"  i sm >>= fun () ->
       let e = Error.KineticError (i,sm) in
       B.failed batch e 

  let start_batch_operation
        ?timeout ?priority client =
    _assert_open client >>= fun () ->
    _assert_no_batch client >>= fun () ->
    let open Session in
    let session = client.session in
    let socket = client.socket in
    let batch_id = session.batch_id in
    let () = session.batch_id <- Int32.succ batch_id in
    let msg = make_start_batch ?timeout ?priority session batch_id in
    network_send_generic I.write I.write_bytes socket msg None I.show_socket session.Session.trace >>= fun () ->
    let batch = B.make session socket batch_id in
    B.add_handler
      batch
      `start_batch_response
      (default_handler batch)
    >>= fun () ->
    Session.incr_sequence session;
    Session.batch_on session;
    Lwt.return batch

  let end_batch_operation (batch:B.t)
    =
    Lwt_log.debug_f ~section "end_batch_operation" >>= fun () ->
    let open B in
    let msg = make_end_batch batch in
    B.add_handler batch `end_batch_response (default_handler batch) >>= fun () ->
    let socket = batch.socket in
    let session = batch.session in
    let trace = session.Session.trace in
    network_send_generic I.write I.write_bytes socket msg None I.show_socket trace >>= fun () ->

    let () = Session.incr_sequence session in
    B.close batch
    >>= fun r ->
    Session.incr_sequence session;
    Session.batch_off session;
    match r with
    | Ok () -> Lwt_result.return batch.socket
    | Error e -> Lwt_result.fail e




  let make_batch_msg session
                     batch_id
                     mt
                     entry
                     ~forced
      =
      let open Message_hmacauth in
      let open Session in
      let command = default_command () in
      let header = default_command_header () in
      header.cluster_version <- Some session.cluster_version;
      header.connection_id <- Some session.connection_id;
      header.sequence <- Some session.sequence;
      header.ack_sequence <- Some session.sequence;

      (* batch_id *)
      header.batch_id <- Some batch_id;

      command.header <- Some header;
      let body = default_command_body () in
      let open Entry in
      let maybe_tag = map_option snd entry.vt in
      let synchronization = Some WRITETHROUGH in
      set_attributes ~ko:(Some entry.key)
                     ~db_version:entry.db_version
                     ~new_version:entry.new_version
                     ~forced
                     ~synchronization
                     ~maybe_tag
                     body;
      command.body <- Some body;
      let m = default_message () in
      header.message_type <- Some mt;
      let command_bytes = Piqirun.to_string(gen_command command) in
      m.command_bytes <- Some command_bytes;
      let hmac = calculate_hmac session.secret command_bytes in
      let hmac_auth = default_message_hmacauth() in
      hmac_auth.identity <- Some session.identity;
      hmac_auth.hmac <- Some hmac;
      m.auth_type <- Some `hmacauth;
      m.hmac_auth <- Some hmac_auth;
      let proto_raw = Piqirun.to_string(gen_message m) in
      proto_raw

  let _assert_batch batch =
    Lwt_log.debug_f "_assert_batch" >>= fun () ->
    match batch.B.error with
    | None   -> Lwt_result.return ()
    | Some e ->
       (* client.closed <- true; *)
       Lwt_result.fail e

  let batch_put
        (batch:B.t) entry
        ~forced
    =
    _assert_batch batch >>=? fun () ->
    Lwt_log.debug_f ~section "batch_put %s" (Entry.show entry) >>= fun () ->
    let open Entry in
    begin
      match entry.vt with
      | None -> Lwt.return_unit (* why would it be None ? *)
      | Some ((v, v_off, v_len), t) -> _assert_value_size batch.B.session v_len
    end
    >>= fun () ->
    let open B in
    let msg = make_batch_msg
                batch.session
                batch.batch_id
                `put
                entry
                ~forced
    in
    let socket = batch.socket in
    let vo = map_option fst entry.vt in
    let session = batch.session in
    let trace = session.Session.trace in
    network_send_generic I.write I.write_bytes socket msg vo I.show_socket trace >>= fun () ->
    let () = B.inc_count batch in
    Session.incr_sequence session;
    Lwt_result.return ()


  let batch_delete
        (batch:B.t)
        entry
        ~forced
    =
    _assert_batch batch >>=? fun () ->
    let open B in
    let msg = make_batch_msg batch.session
                             batch.batch_id
                             `delete
                             entry
                             ~forced
    in
    let socket = batch.socket in
    let session = batch.session in
    let trace = session.Session.trace in
    network_send_generic I.write I.write_bytes socket msg None I.show_socket trace >>= fun () ->
    let () = B.inc_count batch in
    Session.incr_sequence session;
    Lwt_result.return ()


  let make_noop ?timeout ?priority session =
    let mb = set_attributes ~ko:None
                            ~db_version:None
                            ~new_version:None
                            ~forced:None
                            ~synchronization:None
                            ~maybe_tag:None
    in
    make_serialized_msg ?timeout ?priority session `noop mb

  (*

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

 *)
  let noop ?timeout ?priority client =
    _assert_open client >>= fun () ->
    _assert_no_batch client >>= fun () ->
    let msg = make_noop ?timeout ?priority client.session in
    let vo = None in
    _call client msg vo >>=? fun (r,vo, _) ->
    assert (vo = None);
    let command = _parse_command r in
    let () = Session.incr_sequence client.session in
    _assert_both r command `noop_response `success

  let make_instant_secure_erase session ~pin =

    let manip body
      =
      let () =
        set_attributes
          ~ko:None
          ~db_version:None
          ~new_version:None
          ~forced:None
          ~synchronization:None
          ~maybe_tag:None
          body
      in
      let pinop = default_command_pin_operation  () in
      let open Command_pin_operation in
      let () = pinop.pin_op_type <- Some `secure_erase_pinop in
      body.pin_op <- Some pinop
    in
    make_pin_auth_serialized_msg
      session
      pin `pinop manip

  let instant_secure_erase ?pin client =
    _assert_open client >>= fun () ->
    _assert_no_batch client >>= fun () ->
    let pin = get_option "" pin in
    let msg = make_instant_secure_erase client.session ~pin in
    let vo = None in
    _call client msg vo >>=? fun(r,vo, _) ->
    assert (vo = None);
    let command = _parse_command r in
    let () = Session.incr_sequence client.session in
    _assert_both r command `pinop_response `success

  let make_download_firmware session =
    (*
        14984113
        authType: HMACAUTH
        hmacAuth {
          identity: 1
          hmac: "\323\240\'\215\267\302\246>*\335A\33461\372x;o\177\364"
        }
        commandBytes: "\n\020\010\000\030\230\361\254\314\262\361\247\341\n \0008\026\022\004\032\002(\001"

        header {
          clusterVersion: 0
          connectionID: 775357505907407000
          sequence: 0
          messageType: SETUP
        }
        body {
          setup {
            firmwareDownload: true
          }
        }

     *)

    let manip body =
      let open Command_setup in
      let setup = default_command_setup () in
      setup.firmware_download <- Some true;
      body.setup <- Some setup
    in
    make_serialized_msg
      session `setup manip


  let download_firmware client slod_data_slice =
    _assert_open client >>= fun () ->
    _assert_no_batch client >>= fun () ->
    let msg = make_download_firmware client.session in
    let vo = Some slod_data_slice in
    _call client msg vo >>=? fun (r, vo, proto_raw) ->
    assert (vo = None);
    let command = _parse_command r in
    let () = Session.incr_sequence client.session in
    _assert_both r command `setup_response `success
    (*

    let p2p_push session (ic,oc) peer operations =
      let msg = make_p2p_push session peer operations in
      network_send oc msg None >>= fun () ->
      network_receive ic >>= fun (r,vo) ->
      _assert_type r `peer2_peerpush_response;
      let () = incr_session session in
      let status = get_status_code r in
      let lwt_fail x = Lwt.fail(Failure x) in
      match status with
      | `success -> Lwt.return_unit
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

  let wrap_socket ?trace ?timeout ?secret ?cluster_version ?max_operation_count_per_batch socket closer =
    let secret =
      match secret with
      | None -> "asdfasdf"
      | Some secret -> secret
    in
    let cluster_version =
      match cluster_version with
      | None -> 0L
      | Some cluster_version -> cluster_version
    in
    handshake secret cluster_version ?trace ?max_operation_count_per_batch socket ?timeout
    >>=? fun session ->
    Lwt_result.return {session; socket; closer; closed = false}

  let dispose (t:client) =
    Lwt_log.debug_f "dispose: %s" ([%show : string list] (t.session.config.ipv4_addresses)) >>= fun () ->
    if not t.closed
    then
      begin
        t.closed <- true;
        t.closer ()
      end
    else Lwt.return_unit

  end
