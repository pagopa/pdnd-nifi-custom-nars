# pdnd-nifi-custom-nars
This repo is intended to store alla NiFi PagoPA custom [NARs](https://nifi.apache.org/docs/nifi-docs/html/developer-guide.html#nars).

This is the main structure:

```
├── README.md
├── nar-x
│   ├── nifi-nar-x-nar
│   │   └── pom.xml
│   ├── nifi-nar-x-processors
│   │   ├── pom.xml
│   │   ├── src
│   │   │   ├── ...
│   └── pom.xml
└── pom.xml
```

In the root there is:
* pom.xml:
    * The main pom containing the NiFi supported version (and of course other global properties). This pom has a parent that is the NiFi NAR bundle one.
* nar-x (there can be more at this level):
    * A custom NAR created with NiFi Maven archetype org.apache.nifi:nifi-processor-bundle-archetype (Highly recommended to use the archetype).
    **WARNING**: When using the archetype, it generates the pom with NiFi NAR Bundle parent, you need to edit this and set the PagoPA pdnd-nifi-custom-nars parent. Other than that, you need to check even the other properties inside the pom.

For base package name you need to use: it.pagopa.pdnd.nifi.[component] where component is a component of NiFi like: processors, services, providers... .
