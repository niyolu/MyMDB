import os
import ipywidgets as widgets
from scraping import scrape_top_movies
import pickle
import sys
import re


DATA_DIR = "./app/moviedata/"
DATA_DIR = "./moviedata/"


def get_file_options():
    files = os.listdir(DATA_DIR)
    max_f = max(map(len, files))
    sizes = [os.path.getsize(os.path.join(DATA_DIR, file)) for file in files]
    return [f"{f} {s//1024}kb" for f, s in zip(files, sizes)]
    # sizes_str = [f"{s//1024}kb" for s in sizes]
    # len_f, len_s = list(map(len, files)), list(map(len, sizes_str))
    # max_f, max_s = max(len_f), max(len_s)
    # files_sizes = sorted(
    #     zip(files, sizes_str),
    #     key=lambda x: int(re.findall("\d+",x[0])[0])
    # )
    # files_sizes_alligned = [f"{a.ljust(max_f)} ({b.rjust(max_s)})" for idx, (lf, ls, (a,b)) in enumerate(zip(len_f, len_s, files_sizes))]
    # return files_sizes_alligned

def scrape_url(url):
    print("scraping url", url)
    raise NotImplementedError()

def init_ui(movies=None):
    movies = movies or []
    num_movies_slider = widgets.IntSlider(5, 0, 250, 5, description="n")
    playlist_toggles = widgets.ToggleButtons(
        options=["Top 250", "Roulette", "Custom"],
        description="playlist",
    )
    url_textbox = widgets.Text(
        value="https://www.imdb.com",
        description="Custom Src",
    )
    scrape_tab = widgets.VBox(children=[
        playlist_toggles, url_textbox])
    file_select = widgets.Dropdown(
        options=get_file_options(), description="Select pickled movies",
        style={'description_width': '150px'}
    )
    strip_file_checkbox = widgets.Checkbox(
        description="Strip file",
        value=False
    )
    import_tab = widgets.VBox([file_select, strip_file_checkbox])
    tab = widgets.Tab(children=[scrape_tab, import_tab])
    tab.set_title(0, 'Webscraper')
    tab.set_title(1, 'File Import')
    start_button = widgets.Button(
        description='Acquire Movies',
    )
    
    config_ui = widgets.VBox([tab, widgets.HBox([start_button, num_movies_slider])])
    
   

    @start_button.on_click
    def execute_import(b):
        n = num_movies_slider.value
        playlist = playlist_toggles.value
        url = url_textbox.value
        file = file_select.value.split(" ")[0]
        source = ["web", "file"][tab.selected_index]
        strip_file = strip_file_checkbox.value
        if source == "web":
            if playlist == "Top 250":
                movies = scrape_top_movies(n)
            elif playlist == "Roulette":
                raise NotImplementedError()
            elif playlist == "Custom":
                scrape_url(url)
            else:
                raise NotImplementedError()
            
        elif source == "file":
            with open(os.path.join(DATA_DIR, file), "rb") as f:
                movies = pickle.load(f)
            if strip_file:
                movies = movies[:n]
                
                
        sys.stdout.write(f"{len(movies)} movies acquired")
                
        config_ui.movies = movies

    return config_ui

if __name__ == "__main__":
    get_file_options()