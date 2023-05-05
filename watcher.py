#!/usr/bin/python3
import sys
import time
from watchdog.observers import Observer
from watchdog.events import *
import ftpsync
import utils
import sync
import logging
if __name__ == "__main__":

    IP = "10.0.0.5"
    PORT = 2121
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    if len(sys.argv) < 3:
        print(sys.argv[0], " localDirectory remoteDirectory")
        exit(-1)
    localDirectory = sys.argv[1] if len(sys.argv) > 1 else '.'
    remoteDirectory = sys.argv[2]  
    ftp = ftpsync.FtpSync(localDirectory, remoteDirectory, IP, PORT)
    ftp.addPrinter(print)
    ftp.start()
    event_handler = sync.SyncEventHandler()
    observer = Observer(timeout=0)
    observer.schedule(event_handler, localDirectory, recursive=True)
    observer.start()
    try:
        last_events = {}
        delay_threshold = int(1e6 * 750)
        while True:

            for fn, event in event_handler.get().items():
                last_events[fn] = event
            current_time = time.time_ns()
            to_process = []
            for fn, (evt, timestamp, filetype) in last_events.items():
                last_update = current_time - timestamp
                if last_update > delay_threshold or filetype == "dir":
                    to_process.append((fn, evt, filetype))
            updated = False
            for (fn, evt, filetype) in to_process:
                del last_events[fn]
                if filetype == "dir":
                    if evt == "deleted": 
                        ftp.deleteDirectoryByPath(fn)
                    elif evt == "created":
                        ftp.createDirectoryByPath(fn)
                    continue
                if evt != "deleted":
                    updated = updated or ftp.uploadIfHashChanged(fn)
                else:
                    updated = True
                    ftp.deleteFileByPath(fn)
            if updated:
                ftp.syncFileHashes()
            time.sleep(0.01)
    except KeyboardInterrupt:
        pass
    finally:
        ftp.stop()
        observer.stop()
        observer.join()

