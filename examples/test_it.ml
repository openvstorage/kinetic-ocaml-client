let so2s = function
  | None -> "None"
  | Some s -> Printf.sprintf "Some(%S)" s

let vco2s = function
  | None -> "None"
  | Some (v, version) -> Printf.sprintf "Some(%S, %s)" v (so2s version)


open Lwt
open Kinetic

let lwt_test name (f:unit -> unit Lwt.t) : bool Lwt.t=
  Lwt_log.debug_f "starting:%s" name >>= fun () ->
  let timeout = 60. (* Simulator isn't that fast (FLUSH | WRITETHROUGH) *) in
  Lwt.catch
     (fun () ->
      Lwt_unix.with_timeout timeout f
      >>= fun () ->
      Lwt.return true
     )
     (fun exn ->
      Lwt_log.info_f ~exn "failing:%s" name >>= fun () ->
      Lwt.return false
     )
  >>= fun r ->
  Lwt_log.debug_f "end of :%s" name >>= fun () ->
  Lwt.return r

let test_get_non_existing session conn =
  Kinetic.get session conn "I do not exist?"
  >>= fun vo ->
  assert (vo = None);
  Lwt.return ()

let test_noop session conn =
  Kinetic.noop session conn

let batch_ops1 session conn : unit Lwt.t=
  Kinetic.start_batch_operation session conn >>= fun batch ->
  let pe = Kinetic.make_entry
             ~key:"xxx"
             ~db_version:None
             ~new_version:None
             (Some "XXX")
  in
  Kinetic.batch_put batch pe  ~forced:(Some true) >>= fun () ->
  let de = Kinetic.make_entry
             ~key:"xxx"
             ~db_version:None
             ~new_version: None
             None
  in
  Kinetic.batch_delete batch de ~forced:(Some true) >>= fun () ->
  Kinetic.end_batch_operation batch >>= fun conn ->
  Lwt.return ()

let batch_ops2 session conn =
  Kinetic.start_batch_operation session conn >>= fun batch ->
  let pe = Kinetic.make_entry
             ~key:"zzz"
             ~db_version:None
             ~new_version:(Some "ZZZ")
             (Some "ZZZ")
  in
  Kinetic.batch_put batch pe ~forced:(Some true) >>= fun () ->
  Kinetic.end_batch_operation batch >>= fun conn ->
  Lwt.return ()


let batch_ops3 session conn =
  Kinetic.start_batch_operation session conn >>= fun batch ->
  let de =
    Kinetic.make_entry
      ~key:"I do not exist"
      ~db_version:None
      ~new_version:None
      None
  in
  Kinetic.batch_delete batch de ~forced:None >>= fun () ->
  Kinetic.end_batch_operation batch >>= fun conn ->
  Lwt.return ()

let test_put_get_delete session conn =
  let rec loop i =
    if i = 1000
    then Lwt.return ()
    else
      let key = Printf.sprintf "x_%05i" i  in
      let value = Printf.sprintf "value_%05i" i in
      let synchronization = Some Kinetic.WRITEBACK in
      Kinetic.put session conn key value
                  ~db_version:None
                  ~new_version:None
                  ~forced:None
                  ~synchronization
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
      assert (vco = None);
      loop (i+1)
  in
  loop 0

let test_put_version session conn =
  let key = "with_version" in
  Kinetic.delete_forced session conn key >>= fun () ->
  let value = "the_value" in
  let version = Some "0" in
  let synchronization = Some Kinetic.FLUSH in
  Kinetic.put session conn key value
              ~new_version:version
              ~db_version:None
              ~forced:(Some true)
              ~synchronization
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
                   ~synchronization
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
  let synchronization = Some Kinetic.WRITEBACK in
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
        ~synchronization
      >>= fun () ->
      loop (i+1)
  in
  loop 0



let range_test session conn =
  fill session conn 1000 >>= fun () ->
  Kinetic.get_key_range
    session conn
    "x" true "y" true true 20
  >>= fun keys ->
  Lwt_io.printlf "[%s]\n" (String.concat "; " keys) >>= fun () ->
  assert (List.length keys = 20);
  assert (List.hd keys= "x_00999");
  Lwt.return ()

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
  let sa = make_socket_address "127.0.0.1" 11000 in
  let t =
    let secret = "asdfasdf" in
    let cluster_version = 0L in
    Lwt_io.with_connection sa
      (fun conn ->
       Kinetic.handshake secret cluster_version conn >>= fun session ->
       let config = Kinetic.get_config session in
       let open Config in
       Lwt_io.printlf "Config:" >>= fun () ->
       Lwt_io.printlf "version: %s" config.version                  >>= fun ()->
       Lwt_io.printlf "wwn:%s" config.world_wide_name               >>= fun ()->
       Lwt_io.printlf "serial_number:%s" config.serial_number       >>= fun ()->
       Lwt_io.printlf "max_key_size:%i" config.max_key_size         >>= fun ()->
       Lwt_io.printlf "max_value_size:%i" config.max_value_size     >>= fun ()->
       Lwt_io.printlf "max_version_size:%i" config.max_version_size >>= fun ()->

       let run_tests tests =
       Lwt_list.map_s
         (fun (test_name, test) ->
          lwt_test test_name
          (fun () -> test session conn)
          >>= fun r ->
          Lwt.return (test_name,r)
         )
         tests
       in
       run_tests
         [
           "get_non_existing",test_get_non_existing;
           "noop", test_noop;
           "put_get_delete", test_put_get_delete;

           "put_version", test_put_version;
           "range_test", range_test;
           "batch_ops1", batch_ops1;
           "batch_ops2", batch_ops2;
           "batch_ops3", batch_ops3;
           (*"peer2peer", peer2peer_test;*)
         ]
       >>= fun results ->
       Lwt_list.iter_s
         (fun (n,r) -> Lwt_io.printlf "%-32s => %b" n r)
         results
      )
  in
  Lwt_log.add_rule "*" Lwt_log.Debug;
  Lwt_main.run t
