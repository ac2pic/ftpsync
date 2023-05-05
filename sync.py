from watchdog.events import *
import sys
import threading
import time
class SyncEventHandler(FileSystemEventHandler):
    """Logs all the events captured."""
    
    def __init__(self):
        self.last_file_event = {}
        self.lock = threading.Lock()

    def on_moved(self, event: FileSystemEvent) -> None:
        dfn = event.src_path
        nfn = event.dest_path
        filetype = self.get_filetype(event)
        with self.lock:
            self.last_file_event[dfn] = ("deleted", time.time_ns(), filetype)
            self.last_file_event[nfn] = ("created", time.time_ns(), filetype)

    def on_created(self, event: FileSystemEvent) -> None:
        fn = event.src_path
        with self.lock:
            filetype = self.get_filetype(event)   
            self.last_file_event[fn] = ("created", time.time_ns(), filetype)
    
    def get_filetype(self, event: FileSystemEvent):
        if event.is_directory:
            return "dir"
        return "file"

    def on_deleted(self, event: FileSystemEvent) -> None:
        fn = event.src_path
        with self.lock:
            filetype = self.get_filetype(event)   
            self.last_file_event[fn] = ("deleted", time.time_ns(), filetype)

    def on_modified(self, event: FileSystemEvent) -> None:
        fn = event.src_path
        with self.lock:
            filetype = self.get_filetype(event)   
            self.last_file_event[fn] = ("modified", time.time_ns(), filetype)

    def get(self):
        new_events = {}
        with self.lock:
            deleted = []
            for key, (evt, time, filetype) in self.last_file_event.items():
                new_events[key] = (evt, int(time), filetype)
            self.last_file_event.clear()
        return new_events
