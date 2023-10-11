import os
import time
import threading
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Diretório raiz a ser monitorado
root_directory = "C:\\rj227\\data_source"

# Pipeline Beam para processar os caminhos absolutos


class PathExtractor(beam.DoFn):
    def process(self, element):
        yield element


class CustomFileSystemEventHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            print(f'Novo diretório criado: {event.src_path}')
        else:
            print(f'Novo arquivo criado: {event.src_path}')


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
        # Aguarde um segundo para evitar consumo excessivo de CPU
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
