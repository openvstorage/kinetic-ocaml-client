let so2s = function
  | None -> "None"
  | Some s -> Printf.sprintf "Some(%S)" s

let vco2s = function
  | None -> "None"
  | Some (v, version) -> Printf.sprintf "Some(%S, %s)" v (so2s version)


open Lwt
open Kinetic

type test_result =
  | Ok
  | Failed of string
  | Skipped

let show_test_result = function
  | Ok -> "Ok"
  | Failed s -> Printf.sprintf "Failed(%S)" s
  | Skipped -> "Skipped"

let lwt_test name (f:unit -> unit Lwt.t) : test_result Lwt.t=
  Lwt_log.debug_f "starting:%s" name >>= fun () ->
  let timeout = 300.
  (* overkill value, but:
     the simulator isn't that fast (FLUSH | WRITETHROUGH)
     and we sometimes need to test the real drives
     via ssh port forwarding.
   *)
  in
  Lwt.catch
     (fun () ->
      Lwt_unix.with_timeout timeout f
      >>= fun () ->
      Lwt.return Ok
     )
     (fun exn ->
      Lwt_log.info_f ~exn "failing:%s" name >>= fun () ->
      Lwt.return (Failed (Printexc.to_string exn))
     )
  >>= fun r ->
  Lwt_log.debug_f "end of :%s" name >>= fun () ->
  Lwt.return r

let test_get_non_existing client =
  Kinetic.get client "I do not exist?"
  >>= fun vo ->
  assert (vo = None);
  Lwt.return ()

let test_put_no_tag client =
  let key = "test_put_no_tag" in
  let value = key in
  let synchronization = Some Kinetic.WRITEBACK in
  Lwt_io.printlf "drive[%S] <- Some %S%!" key value >>= fun () ->
  Kinetic.put
    client
    key value
    ~db_version:None
    ~new_version:None
    ~forced:None
    ~tag:None
    ~synchronization
  >>= fun () ->
  Lwt.return ()

let test_noop client = Kinetic.noop client


let batch_single_put client =
  Kinetic.start_batch_operation client >>= fun batch ->
  let v = "ZZZ" in
  let tag = Kinetic.make_sha1 v in
  let pe = Kinetic.make_entry
             ~key:"zzz"
             ~db_version:None
             ~new_version:(Some "ZZZ")
             (Some (v,tag))
  in
  Kinetic.batch_put batch pe ~forced:(Some true) >>= fun () ->
  Kinetic.end_batch_operation batch >>= fun conn ->
  Lwt.return ()

let batch_test_put_delete client : unit Lwt.t=
  Kinetic.start_batch_operation client
  >>= fun batch ->
  let v = "XXX" in
  let tag = Kinetic.make_sha1 v in
  let pe = Kinetic.make_entry
             ~key:"xxx"
             ~db_version:None
             ~new_version:None
             (Some (v, tag))
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


let batch_delete_non_existing client =
  Kinetic.start_batch_operation client >>= fun batch ->
  let de =
    Kinetic.make_entry
      ~key:"I do not exist"
      ~db_version:None
      ~new_version:None
      None
  in
  Kinetic.batch_delete batch de ~forced:(Some true) >>= fun () ->
  Lwt_log.debug_f "delete sent" >>= fun () ->
  Kinetic.end_batch_operation batch >>= fun (ok, conn) ->
  assert (ok = true);
  Lwt_log.debug_f "end_batch sent" >>= fun () ->
  Lwt.return ()

let batch_3_puts client : unit Lwt.t =
  Kinetic.start_batch_operation client
  >>= fun batch ->
  let add_put key v =
    let pe =
      let tag = Kinetic.make_sha1 v in
      Kinetic.make_entry
             ~key
             ~db_version:None
             ~new_version:None
             (Some (v,tag))
    in
    Kinetic.batch_put batch pe ~forced:(Some true)
  in
  let make_key i = Printf.sprintf "batch_test_3_puts:key_%03i" i in
  let key0 = make_key 0 in
  let key1 = make_key 1 in
  let key2 = make_key 2 in
  add_put key0 key0 >>= fun () ->
  add_put key1 key1 >>= fun () ->
  add_put key2 key2 >>= fun () ->
  Kinetic.end_batch_operation batch >>= fun (ok, conn) ->
  assert (ok = true);
  Lwt.return_unit

let test_crc32 client =
  let key = "test_crc32_key" in
  let value = key in
  (*let tag = Kinetic.Crc32 0xEAE10D3Al in*)
  let tag = Kinetic.Crc32 0x0l in
  let synchronization = Some Kinetic.WRITEBACK in
  Kinetic.put client key value
              ~db_version:None
              ~new_version:None
              ~forced:None
              ~tag:(Some tag)
              ~synchronization
              >>= fun () ->
  Lwt.return ()


let test_put_get_delete client =
  let rec loop i =
    if i = 400
    then Lwt.return ()
    else
      let key = Printf.sprintf "x_%05i" i  in
      let value = Printf.sprintf "value_%05i" i in
      let synchronization = Some Kinetic.WRITEBACK in
      Lwt_io.printlf "drive[%S] <- Some %S%!" key value >>= fun () ->
      let tag = Kinetic.make_sha1 value in

      Kinetic.put client key value
                  ~db_version:None
                  ~new_version:None
                  ~forced:None
                  ~tag:(Some tag)
                  ~synchronization
      >>= fun () ->
      Kinetic.get client key >>= fun vco ->
      Lwt_io.printlf "drive[%S]=%s%!" key (vco2s vco) >>= fun () ->
      let () = match vco with
      | None -> failwith "should be present"
      | Some (value2, version) ->
         begin
           assert (value = value2);
           assert (version = Some "");
         end
      in
      Kinetic.delete_forced client key >>= fun () ->
      Lwt_io.printlf "deleted %S" key >>= fun () ->
      Kinetic.get client key >>= fun vco ->
      Lwt_io.printlf "drive[%S]=%s" key (vco2s vco) >>= fun () ->
      assert (vco = None);
      loop (i+1)
  in
  loop 0

let test_put_largish client =
  let key = "largish" in
  let value = Bytes.create 100_000 in
  let tag = Kinetic.make_sha1 value in
  let synchronization = Some Kinetic.FLUSH in
  Kinetic.put client key value
              ~new_version:None
              ~db_version:None
              ~forced:(Some true)
              ~synchronization
              ~tag:(Some tag)
  >>=fun () ->
  Kinetic.get client key >>= fun vco ->
  assert (vco <> None);
  Lwt.return ()

let test_put_version client =
  let key = "with_version" in
  Kinetic.delete_forced client key >>= fun () ->
  let value = "the_value" in
  let tag = Kinetic.make_sha1 value in
  let version = Some "0" in
  let synchronization = Some Kinetic.FLUSH in
  Kinetic.put client key value
              ~new_version:version
              ~db_version:None
              ~forced:(Some true)
              ~synchronization
              ~tag:(Some tag)
  >>= fun () ->
  Kinetic.get client key >>= fun vco ->
  Lwt_io.printlf "vco=%s" (vco2s vco) >>= fun () ->
  begin
    Lwt.catch
      (fun () ->
       let new_version = Some "1" in
       let value2 = "next_value" in
       let tag2 = Kinetic.make_sha1 value2 in
       Kinetic.put client key value2
                   ~db_version:new_version ~new_version
                   ~forced:None
                   ~synchronization
                   ~tag:(Some tag2)
       >>= fun () ->
       Lwt.return false
      )
      (fun exn -> Lwt.return true)
  end
  >>= function
  | false -> Lwt.fail (Failure "bad behaviour")
  | true ->
     Kinetic.get client key >>= fun vco2 ->
     Lwt_io.printlf "vco2=%s" (vco2s vco2) >>= fun () ->
     Lwt.return ()

let fill client n =
  let synchronization = Some Kinetic.WRITEBACK in
  let rec loop i =
    if i = n
    then Lwt.return ()
    else
      let key = Printf.sprintf "x_%05i" i in
      let v = Printf.sprintf "value_%05i" i in
      let tag = Kinetic.make_sha1 v in
      begin
        if i mod 100 = 0 then Lwt_io.printlf "i:%i" i else Lwt.return ()
      end
      >>= fun ()->
      Kinetic.put
        client key v
        ~db_version:None
        ~new_version:None
        ~forced:(Some true)
        ~synchronization
        ~tag:(Some tag)
      >>= fun () ->
      loop (i+1)
  in
  loop 0



let range_test client =
  fill client 1000 >>= fun () ->
  Kinetic.get_key_range
    client
    "x" true "y" true false 20
  >>= fun keys ->
  Lwt_io.printlf "[%s]\n%!" (String.concat "; " keys) >>= fun () ->
  assert (List.length keys = 20);
  assert (List.hd keys= "x_00000");
  Lwt.return ()

let range_test_reverse client =
  fill client 1000 >>= fun () ->
  (* note the order, which differs from the specs *)
  Kinetic.get_key_range client "x" true "y" true true 20
  >>= fun keys ->
  Lwt_io.printlf "[%s]\n%!" (String.concat "; " keys) >>= fun () ->
  assert (List.length keys = 20);
  assert (List.hd keys= "x_00999");
  Lwt.return ()


let get_capacities_test client =
  Kinetic.get_capacities client >>= fun (cap, fill_rate) ->
  Lwt_io.printlf "(%Li,%f)" cap fill_rate >>= fun () ->
  Lwt.return_unit


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

let maybe_init_ssl =
  let ok = ref false in
  (fun () ->
    if !ok then ()
    else
      begin
        Ssl_threads.init ();
        Ssl.init ~thread_safe:true ();
        ok := true
      end
  )

let run_with_client ip port trace ssl f =
  let ctx =
    if ssl then
      begin
        maybe_init_ssl ();
        let protocol = Ssl.TLSv1_2 in
        let ctx = Ssl.create_context protocol Ssl.Client_context in
        Some ctx
      end
    else
      None
  in

  let t =
    Lwt_log.debug_f
      "ip:%S port:%i trace:%b" ip port trace
    >>= fun () ->
    Kinetic.with_client ?ctx ~ip ~port ~trace f
  in
  Lwt_log.add_rule "*" Lwt_log.Debug;
  Lwt_main.run t

let instant_secure_erase ip port trace =

  let f client =
    let session = Kinetic.get_session client in
    let config = Kinetic.get_config session in
    Lwt_io.printlf "%s" (Config.show config) >>= fun () ->
    Kinetic.instant_secure_erase client >>= fun () ->
    Lwt.return_unit
  in

  let ssl = true in
  run_with_client ip port trace ssl f


let download_firmware ip port trace file_name =
  let f client =
    let session = Kinetic.get_session client in
    let config = Kinetic.get_config session in
    Lwt_io.printlf "%s" (Config.show config) >>= fun () ->
    Lwt_unix.stat file_name >>= fun stat ->
    let size = stat.Lwt_unix.st_size in
    let slod = Bytes.create size in
    Lwt_io.with_file
      ~mode:Lwt_io.input file_name
      (fun ic ->
        Lwt_io.read_into_exactly ic slod 0 size
      )
    >>= fun () ->
    Lwt_io.printlf "update has %i bytes%!" size >>= fun () ->
    Kinetic.download_firmware client slod >>= fun () ->
    Lwt.return_unit
  in
  let ssl = true in
  run_with_client ip port trace ssl f

let run_tests ip port trace ssl filter =
  let f client =
    let run_tests tests =
      Lwt_list.map_s
        (fun (test_name, test) ->
          if filter = [] || List.mem test_name filter
          then
            begin
              lwt_test test_name (fun () -> test client)
              >>= fun r ->
              Lwt.return (test_name,r)
            end
          else
            Lwt.return (test_name, Skipped)
        )
        tests
    in
    run_tests [
        "get_non_existing",test_get_non_existing;
        "noop", test_noop;
        "put_get_delete", test_put_get_delete;
        "put_version", test_put_version;
        "put_largish", test_put_largish;
        "range_test", range_test;
        "range_test_reverse", range_test_reverse;

        "batch_single_put", batch_single_put;
        "batch_put_delete", batch_test_put_delete;
        "batch_delete_non_existing", batch_delete_non_existing;
        "batch_3_puts", batch_3_puts;

        "crc32", test_crc32;
        "get_capacities", get_capacities_test;
        (* "put_no_tag", test_put_no_tag; *)
        (*"peer2peer", peer2peer_test;*)
      ]
    >>= fun results ->
    Lwt_list.iter_s
      (fun (n, r) -> Lwt_io.printlf "%-32s => %s"  n (show_test_result r))
      results
  in
  run_with_client ip port trace ssl f


module Cli = struct
  open Cmdliner
  let ip =
    Arg.(value
         & opt string "::1"
         & info ["h";"host"] ~docv:"HOST" ~doc:"the host to connect with")

  let port default =
    let doc = "tcp $(docv)" in
    Arg.(value
         & opt int default
         & info ["p"; "port"] ~docv:"PORT" ~doc)

  let trace =
    Arg.(value
         & flag
         & info ["trace"]
                ~doc:"trace sent/received messages")

  let ssl =
    Arg.(value
         & flag
         & info ["ssl"]
                ~doc:"perform communication using ssl sockets")

  let filter =
    Arg.(value
         & opt_all string []
         & info ["filter"] ~doc:"run test(s) matching"
    )

  let file =
    Arg.(required
         & opt (some non_dir_file) None
         & info ["file"] ~docv:"FILE")


  let run_tests_cmd =
    let open Term in
    (pure run_tests
     $ ip
     $ port 8123
     $ trace
     $ ssl
     $ filter

    ),
    info
      "run-tests"
      ~doc:"runs tests"

  let instant_secure_erase_cmd =
    let open Term in
    (pure instant_secure_erase
     $ ip
     $ port 8443
     $ trace
    ),
    info
      "instant-secure-erase"
      ~doc:"erases all data from the drive. Warranty void. You have been warned"

  let download_firmware_cmd =
    let open Term in
    (pure download_firmware
     $ ip
     $ port 8443
     $ trace
     $ file
    ),
    info
      "download-firmware"
      ~doc:"flash new firmware on drive. Warranty void. You have been warned."

  let default () =
    Printf.printf "an ocaml client for kinetic drives: tester & cli %!"

  let default_cmd =
    Term.(const default $ const ()),
    Term.info "" ~doc:"what's possible?"

end

let () =
  let open Cli in
  let open Cmdliner in
  let cmds =
    [run_tests_cmd;
     instant_secure_erase_cmd;
     download_firmware_cmd;
    ]
  in

  match Term.eval_choice default_cmd cmds with
  | `Error _ -> exit 1
  | _ -> exit 0
