GZM GTFS
========

Creates a single, merged GTFS file for [Zarząd Transport Metropolitalnego Górnośląsko-Zagłębiowskiej Metropolii](https://www.metropoliaztm.pl/)
from files published at <https://otwartedane.metropoliagzm.pl/dataset/rozklady-jazdy-i-lokalizacja-przystankow-gtfs-wersja-rozszerzona>.


Running
-------

GZM GTFS is written in Python with the [Impuls framework](https://github.com/MKuranowski/Impuls).

To set up the project, run:

```terminal
$ python -m venv .venv
$ . .venv/bin/activate
$ pip install -Ur requirements.txt
```

Then, run:

```terminal
$ python gzm_gtfs.py
```

The resulting schedules will be put in a file called `gzm.zip`.

See `python gzm_gtfs.py --help` for a list of all available options.


License
-------

_GZMGTFS_ is provided under the MIT license, included in the `LICENSE` file.
