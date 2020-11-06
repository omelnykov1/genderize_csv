# Genderize CSV
Crediting: jholtmann - Original creater of this script

**Python genderize.io script**

This script takes a single column CSV file with a header and feeds the names to genderize.io. It outputs a CSV file with the name, gender, probability, and count of every name.

### Usage:
```sh
python genderize.py [-h] -i INPUT -o OUTPUT [-k KEY] [-c] [-ns] [-nh]
```

```
optional arguments:
  -h, --help            show this help message and exit
  -k KEY, --key KEY     API key
  -c, --catch           Try to handle errors gracefully
  -a, --auto            Automatically complete gender for identical names
  -nh, --noheader       Input has no header row

required arguments:
  -i INPUT, --input INPUT
                        Input file name
  -o OUTPUT, --output OUTPUT
                        Output file name
```

#### Flag details
- _key_:       specify API key [required for 1000+ names]
- _catch_:     try to gracefully catch and handle errors [recommended]
- _auto_:      only request genders for unique names, then autocomplete the duplicates. May significantly lower API usage (by 50% in big test file, for example) [see "Note" for more info]
- _noheader_:  use if input file has no header row

### Test usage:
```
python genderize.py -i test/test.csv -o test/out.csv --catch
```

### Note:
- API key (https://store.genderize.io) is required when requesting more than 1000 names a month.
- Warning: If an error occurs while executing script with the _auto_ argument, no name-matching will occur. The _.tmp_ file will have all the unique names processed to that point, but the script does not yet support picking up from where it was where an error occured! If an error occurs while processing names without the _auto_ argument, you can just remove the processed names from the input file and continue, this is not possible when using the _auto_ argument.

### Requires:
Required module can be found in "dep" folder or pypi link (see "Dependencies")
```
pip install Genderize-0.1.5-py3-none-any.whl
```
Python 3.* (Known working: 3.6.1)

#### Dependencies:
- https://pypi.python.org/pypi/Genderize / https://github.com/SteelPangolin/genderize

### Features:
- Bulk processing (tested with 600,000+ names)
- Estimates remaining time
- Writes data after processing 10 names so little data is lost if an error occurs 
- Support for genderize.io API key (allows processing of more than 1000 names /mo).

### To-do:
- Add ability to search multi-column CSV file for column with specific header [easy]
- Add support for alternate output formats [moderate]
- Add support for using file as a module [easy]
- Add ability to pick up name processing from data in .tmp file if error occurs while using _auto_ argument [hard]
- ~~Add support for optionally caching gender responses and searching through them for identical names before asking genderize for the data. This would lower API key request usage.~~ DONE
- ~~Catch 502 bad gateway error and retry the request. Currently the program will just catch the error, print it, and exit.~~ DONE
- ~~Add better command line flags~~ DONE

#### "Chunks" explanation:
The Python Genderize client used limits requests to 10 names. To work around this, the code breaks the list of names down into chunks of 10. This approach also has the benefit of preventing data loss in case of a crash/server error as the results are written to the output file every 10 names.
