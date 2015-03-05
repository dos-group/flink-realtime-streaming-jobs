In case the repository containing lingpipe-4.1.0 is offline download it

    wget http://lingpipe-download.s3.amazonaws.com/lingpipe-4.1.0.jar

and install to the local maven repository

    mvn install:install-file -DgroupId=com.aliasi -DartifactId=lingpipe -Dversion=4.1.0 -Dpackaging=jar -DgeneratePom=true -Dfile=lingpipe-4.1.0.jar
