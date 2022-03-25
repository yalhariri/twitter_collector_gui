CONTENTS
--------

* Introduction
* OS environment
* Installation
* USING LAMLEM


INTRODUCTION
------------

Lamlem is an innovative Twitter data collection tool that helps users to collect twitter data straightaway. For a smooth and bright start, please read this file and if you have any question, please feel free to contact us.


OS ENVIRONMENT
--------------

The current version of the tool works on Mac OS and Linux. For some technical reasons, it cannot work on MS Windows OS. We are working on this to make it support MS platforms too.


INSTALLATION
------------

There is no specific installation requirement for this tool. However, there are some requirements:

1. Python3 with the following library installed in the system:
	1.1 twython>=3.6.0
	1.2. pysolr
	1.3. subprocess
	1.4. requests
	1.5. nltk
	1.6. urllib
	1.7. JSON
	1.8. altair
	1.9. matplotlib
	1.10. wordcloud

2. Streamlit, you can install it by entering the command: pip install streamlit.


3. Apache Solr for the indexing. You can download it from:
https://lucene.apache.org/solr/downloads.html

Please take note of the location where Solr is installed as you need to add it in this tool.
You do not need any further work in Solr. Even, you do not need to start it from the terminal.

4. Twitter API keys. You need to register and record the Twitter API from:
https://developer.twitter.com/en/apps


USING LAMLEM
------------

To start the application, you need to run the shell script 'run_me.sh' from the terminal
To use the tool, please refer to the video from the link:
https://youtu.be/_xykbQqjVnQ

The information stored will be available and accessible both from the application and directly from the Solr interface.
