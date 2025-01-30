# Point of Interest Community Broker

The Point of Interest Community Broker is a transient alert software (Rubin Observatory (LSST) Alert and Community Broker) that is currently tested with the ZTF alert stream.
Incoming alerts will be processed, annotated, classified and forwarded.

For a general overview of LSST Community Broker, see https://www.lsst.org/scientists/alert-brokers


This repository contains the backend (data processing) for the Point of Interest broker.
The Point of Interest broker can be found at https://poibroker.uantof.cl/


## Motivation
 
Current and especially upcoming all-sky time-domain surveys, such as LSST, will deliver a vast amount of data each night, requiring for the developent of flexible, straightforward tools for the analysis, selection and forwarding of information regarding astrophysical transients and variable objects. 
 
Our alert broker, called *Point of Interest*, is tailored towards the needs of astronomers looking for updated observations of variable stars in specific on-sky regions. Developed by a small team at Vanderbilt University, where I'm the main developer responsible for this project, this *Point of Interest*' alert broker should enable users to get updates on variable star observations from a straightforward, user-friendly web service. Data are processed in real time by big data/ machine learning algorithms and will be immediately available to the user community.


*Point of Interest* differs from other brokers in the focus on updates on variable stars, thus running a rather specific than the full analysis chain of streamed data. As a consequence, the broker is rather lightweight. *Point of Interest* users are encouraged to design their own on-sky regions they want receive updates for (such as for planned follow-up campaigns) or select from a list of on-sky regions which are particularly interesting for variable star observers, such as stellar streams, globular clusters and dwarf galaxies.

