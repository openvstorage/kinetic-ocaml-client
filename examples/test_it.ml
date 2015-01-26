let so2s = function
  | None -> "None"
  | Some s -> Printf.sprintf "Some(%S)" s

open Lwt
open Kinetic

let put_get_delete_test session conn =
  let rec loop i =
    if i = 1000
    then Lwt.return ()
    else
      let key = Printf.sprintf "x_%05i" i  in
      let value = Printf.sprintf "value_%05i" i in
      Kinetic.set session conn key (Some value) >>= fun () ->
      Kinetic.get session conn key >>= fun vo ->
      Lwt_io.printlf "drive[%S]=%s" key (so2s vo) >>= fun () ->
      assert (Some value = vo);
      Kinetic.set session conn key None >>= fun () ->
      Lwt_io.printlf "deleted %S" key >>= fun () ->
      Kinetic.get session conn key >>= fun vo ->
      Lwt_io.printlf "drive[%S]=%s" key (so2s vo) >>= fun () ->
      loop (i+1)
  in
  loop 0


let fill session conn n =
  let rec loop i =
    if i = n
    then Lwt.return ()
    else
      let key = Printf.sprintf "x_%05i" i in
      let v = Printf.sprintf "value_%05i" i in
      let vo = Some v in
      Kinetic.set session conn key vo >>= fun () ->
      loop (i+1)
  in
  loop 0



let range_test session conn =
  Kinetic.get_key_range session conn
                        "x" true "y" true true 20
  >>= fun keys ->
  Lwt_io.printlf "[%s]\n" (String.concat "; " keys)


(*
let peer2peer_test session conn =
  let peer = "192.168.11.102", 8000, false in
  let operations = [
      ("x_00000", None);
      ("x_00010", None);
      ("x_00100", Some "y_00100");
    ]
  in
  Kinetic.p2p_push session conn peer operations
 *)


let () =
  let make_socket_address h p = Unix.ADDR_INET(Unix.inet_addr_of_string h, p) in
  let sa = make_socket_address "127.0.0.1" 9000 in
  let t =
    let secret = "asdfasdf" in
    let cluster_version = 0L in
    Lwt_io.with_connection sa
      (fun conn ->
       Kinetic.handshake secret cluster_version conn
       >>= fun session ->
       (*
       put_get_delete_test session conn >>= fun () ->
       fill session conn 1000 >>= fun () ->
       Lwt_io.printlf "range:" >>= fun () ->
       range_test session conn >>= fun () ->
        *)
       let batch_id = 65l in
       Kinetic.start_batch_operation session conn batch_id      >>= fun batch ->
       Kinetic.batch_put session conn batch "xxx" (Some "XXX")  >>= fun () ->
       (*
Kinetic.batch_put session conn batch"xxxx" (Some "XXXX") >>= fun () ->
        *)
       Kinetic.end_batch_operation session batch >>= fun conn ->

(*
       Kinetic.noop session conn >>= fun () ->
       peer2peer_test session conn
 *)
       Lwt.return ()
      )
  in
  Lwt_log.add_rule "*" Lwt_log.Debug;
  Lwt_main.run t
