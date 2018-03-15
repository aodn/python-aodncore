
### Running tests on a Mac may cause problems such as:

```
OSError: [UT_OPEN_DEFAULT] Failed to open UDUNITS-2 XML unit database
```

This is fixed by setting the path to the XML unit database (xml file)

##### Example in .bash_profile
```
export UDUNITS2_XML_PATH="/opt/local/share/udunits/udunits2.xml"
```

