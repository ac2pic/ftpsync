#!/usr/bin/python3
import os
import sys
import time
import logging
import hashlib
import json
import io
import ast
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent
from logging import StreamHandler
from functools import reduce
from operator import concat 


import ftpsync
import utils


class SyncEventHandler(FileSystemEventHandler):
    """Logs all the events captured."""
    
    def __init__(self):
        super().__init__()
        logging.getLogger().addHandler(StreamHandler(sys.stdout))
        self.logger = logging.getLogger()

    def on_moved(self, event: FileSystemEvent) -> None:
        super().on_moved(event)
        what = "directory" if event.is_directory else "file"
        self.logger.info("Moved %s: from %s to %s", what, event.src_path, event.dest_path)

    def on_created(self, event: FileSystemEvent) -> None:
        super().on_created(event)

        what = "directory" if event.is_directory else "file"
        self.logger.info("Created %s: %s", what, event.src_path)

    def on_deleted(self, event: FileSystemEvent) -> None:
        super().on_deleted(event)
        what = "directory" if event.is_directory else "file"
        self.logger.info("Deleted %s: %s", what, event.src_path)
        ftp.getRemotePath(event.src_path)
    def on_modified(self, event: FileSystemEvent) -> None:
        super().on_modified(event)
        if event.is_directory:
            return
        what = "directory" if event.is_directory else "file"

        self.logger.info("Modified %s: %s", what, event.src_path)

    def on_closed(self, event: FileSystemEvent) -> None:
        super().on_closed(event)
        self.logger.info("Closed file: %s", event.src_path)

    def on_opened(self, event: FileSystemEvent) -> None:
        super().on_opened(event)
        self.logger.info("Opened file: %s", event.src_path)
         
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
    global ftp
    ftp = ftpsync.FtpSync(localDirectory, remoteDirectory, IP, PORT)
    ftp.start()
    event_handler = SyncEventHandler()
    observer = Observer()
    observer.schedule(event_handler, localDirectory, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        ftp.stop()
        observer.stop()
        observer.join()

