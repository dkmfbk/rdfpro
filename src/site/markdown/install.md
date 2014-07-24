
RDFpro installation
===================

The RDFpro command line tool is packaged as a self-contained, cross-platform Java application. Download it from the following links:

  * [rdfpro-dist-0.1-bin.tar.gz](http://fracor.bitbucket.org/rdfpro/mvnrepo/eu/fbk/rdfpro/rdfpro-dist/0.1/rdfpro-dist-0.1-bin.tar.gz)
  * [rdfpro-dist-0.1-bin.zip](http://fracor.bitbucket.org/rdfpro/mvnrepo/eu/fbk/rdfpro/rdfpro-dist/0.1/rdfpro-dist-0.1-bin.zip)

Then, unpack the compressed archive in the location you prefer. The following, optional steps are recommended:

  * Add the rdfpro directory to your PATH variable, so that the `rdfpro` script can be called without specifying its full path.

  * Make sure you have the `sort`, `gzip`, `bzip2` utilities (also `xz` and `7za` are useful).
    On a Mac/Linux machine they should be already installed.
    On Windows, you can get them from the [GnuWin](http://gnuwin32.sourceforge.net/) project.

  * For better performances, you may install the multi-threaded versions of `gzip` and `bzip2`, called [`pigz`](http://zlib.net/pigz/) and [`pbzip2`](http://compression.ca/pbzip2/)
    These utilities are available also for Windows (for `pigz` see [here](http://sourceforge.net/projects/pigzforwindows/) and [here](http://blog.kowalczyk.info/software/pigz-for-windows.html)).
    After their installation, you have to configure RDFpro to use them by setting the following environment variables (an information message is emitted if the configuration is successfully picked up by the tool):

            RDFPRO_CMD_GZIP = path_to_pigz_executable
            RDFPRO_CMD_BZIP2 = path_to_pbzip2_executable

You may test whether RDFpro has been correctly installed by executing the following command, which should display the tool version and other relevant system information:

    rdfpro -v
