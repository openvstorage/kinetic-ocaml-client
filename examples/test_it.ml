let so2s = function
  | None -> "None"
  | Some s -> Printf.sprintf "Some(%S)" s

let vco2s = function
  | None -> "None"
  | Some (v, version) -> Printf.sprintf "Some(%S, %s)" v (so2s version)


open Lwt
open Kinetic

let put_get_delete_test session conn =

  let rec loop i =
    if i = 1000
    then Lwt.return ()
    else
      let key = Printf.sprintf "x_%05i" i  in
      let value = Printf.sprintf "value_%05i" i in
      Kinetic.put session conn key value
                  ~db_version:None
                  ~new_version:None
                  ~forced:(Some true)
      >>= fun () ->
      Kinetic.get session conn key >>= fun vco ->
      Lwt_io.printlf "drive[%S]=%s" key (vco2s vco) >>= fun () ->
      let () = match vco with
      | None -> failwith "should be present"
      | Some (value2, version) ->
         begin
           assert (value = value2);
           assert (version = Some "");
         end
      in
      Kinetic.delete_forced session conn key >>= fun () ->
      Lwt_io.printlf "deleted %S" key >>= fun () ->
      Kinetic.get session conn key >>= fun vco ->
      Lwt_io.printlf "drive[%S]=%s" key (vco2s vco) >>= fun () ->
      loop (i+1)
  in
  loop 0

let put_version_test session conn =
  let key = "with_version" in
  Kinetic.delete_forced session conn key >>= fun () ->
  let value = "the_value" in
  let version = Some "0" in
  Kinetic.put session conn key value
              ~new_version:version
              ~db_version:None
              ~forced:(Some true)
  >>= fun () ->
  Kinetic.get session conn key >>= fun vco ->
  Lwt_io.printlf "vco=%s" (vco2s vco) >>= fun () ->
  begin
    Lwt.catch
      (fun () ->
       let new_version = Some "1" in
       Kinetic.put session conn key "next_value"
                   ~db_version:new_version ~new_version
                   ~forced:None
       >>= fun () ->
       Lwt.return false
      )
      (fun exn -> Lwt.return true)
  end
  >>= function
  | false -> Lwt.fail (Failure "bad behaviour")
  | true ->
     Kinetic.get session conn key >>= fun vco2 ->
     Lwt_io.printlf "vco2=%s" (vco2s vco2) >>= fun () ->
     Lwt.return ()

let fill session conn n =
  let rec loop i =
    if i = n
    then Lwt.return ()
    else
      let key = Printf.sprintf "x_%05i" i in
      let v = Printf.sprintf "value_%05i" i in
      Kinetic.put
        session conn key v
        ~db_version:None
        ~new_version:None
        ~forced:(Some true)
      >>= fun () ->
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

       put_get_delete_test session conn >>= fun () ->
       put_version_test session conn >>= fun () ->
       fill session conn 1000 >>= fun () ->
       Lwt_io.printlf "range:" >>= fun () ->
       range_test session conn >>= fun () ->

       Kinetic.start_batch_operation session conn >>= fun batch ->
       let pe = Kinetic.make_entry
                     ~key:"xxx"
                     ~db_version:None
                     ~new_version:None
                     (Some "XXX")
       in
       Kinetic.batch_put batch pe ~forced:(Some true)
       >>= fun () ->
       let de = Kinetic.make_entry
                  ~key:"xxx"
                  ~db_version:None
                  ~new_version: None
                  None
       in
       Kinetic.batch_delete batch de ~forced:(Some true)
       >>= fun () ->

       Kinetic.end_batch_operation batch >>= fun conn ->

(*
       Kinetic.noop session conn >>= fun () ->
       peer2peer_test session conn
 *)
       Lwt.return ()
      )
  in
  Lwt_log.add_rule "*" Lwt_log.Debug;
  Lwt_main.run t
