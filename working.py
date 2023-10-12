import os
import time
import threading
import apache_beam as beam
import shutil
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from apache_beam.options.pipeline_options import PipelineOptions
from ftplib import FTP

# Root folder to be monitored
root_directory = "C:\\rj227\\data_source"

# Pipeline Beam to process absolute paths


class PathExtractor(beam.DoFn):
    def process(self, element):
        yield element


class CustomFileSystemEventHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            print(f'New folder has been created: {event.src_path}')
        else:
            print(f'New file has been created: {event.src_path}')
            start_time = time.time()
            while True:
                current_size = os.path.getsize(event.src_path)
                time.sleep(5)  # Wait 1 second
                new_size = os.path.getsize(event.src_path)

                if current_size == new_size or time.time() - start_time > 10:
                    print(f"File {event.src_path} is ready for processing.")

                    # FTP
                    ftp = FTP('server')
                    ftp.login('user', 'password')
                    with open(event.src_path, 'rb') as file:
                        try:
                            ftp.storbinary(
                                'STOR ' + os.path.basename(event.src_path), file)
                            ftp.quit()
                        except:
                            print(f"File cannot be uploaded.")
                            ftp.quit()

                    # Copy to another folder
                    # try:
                    #     destination_folder = 'C:\\destination_folder'
                    #     shutil.copy(event.src_path, os.path.join(
                    #         destination_folder, os.path.basename(event.src_path)))
                    # except:
                    #     print(f"File cannot be moved.")


def run_beam_pipeline():
    options = PipelineOptions([])
    with beam.Pipeline(options=options) as p:
        _ = (
            p
            | "Generate paths" >> beam.Create([root_directory])
            | "Find paths" >> beam.FlatMap(lambda path: [os.path.join(dp, f) for dp, dn, filenames in os.walk(path) for f in filenames])
            | "Extract paths" >> beam.ParDo(PathExtractor())
        )


def start_beam_pipeline():
    while True:
        # Wait a second to avoid excessive CPU usage
        time.sleep(1)
        run_beam_pipeline()


if __name__ == "__main__":
    event_handler = CustomFileSystemEventHandler()

    observer = Observer()
    observer.schedule(event_handler, path=root_directory, recursive=True)
    observer.start()

    beam_thread = threading.Thread(target=start_beam_pipeline)
    beam_thread.daemon = True
    beam_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
