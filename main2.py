import time
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

data_source_dir = 'C:\\rj227\\data_source'


class MyHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            file_path = event.src_path
            if file_path.endswith(('.mp4', '.txt')):
                print(f"Found new file: {file_path}")

            # Espera até que o tamanho do arquivo pare de mudar ou até um limite de tempo (por exemplo, 5 segundos)
            start_time = time.time()
            while True:
                current_size = os.path.getsize(file_path)
                time.sleep(5) 
                new_size = os.path.getsize(file_path)

                if current_size == new_size or time.time() - start_time > 10:
                    print(f"File {file_path} is ready for processing.")
                    # Provavelmente aqui teriamos o código para processar esse arquivo e enviá-lo para outra pasta.
                    # Coisas do FTP.
                    break


def run():
    event_handler = MyHandler()
    observer = Observer()
    observer.schedule(event_handler, path=data_source_dir, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


if __name__ == '__main__':
    run()
