This is a template project for team project of 2AMD15 2023.

Various comments in the main.py file indicate where functionality needs to be implemented.
These comments are marked TODO.

You are allowed to change the layout of the main.py file (such as method names and signatures) in any way you like,
and you can add new functionality (possibly in new .py files). Do make sure that there exists a file called "main.py"
which contains the 'main module statement' of the form "if __name__ == '__main__':".

You can ZIP the main.py file along with any other .py files using any common compression tool. The archive should
be called app.zip and can be uploaded as such to the server.

Good luck on the project!

------- GenVec.jar -------

Hi there! This short readme is supposed to accompany a file called GenVec.jar.
Genvec.jar can be used to generate a dataset containing labelled vectors for the 2AMD15 project.

GenVec takes one required- and two optional runtime parameters, namely:
	-	your group number (without leading a leading zero, so 9 instead of 09)
	-	the number of vectors to generate (default value is 40)
	- 	the length of each vector (default value is 20)

You can run GenVec, after installing an up-to-date version of Java, by running the following command:
	java -jar GenVec.jar 23 [number-of-vectors] [length-of-vectors]

from a command line in the directory which contains GenVec.jar. This will (over)write a file named vectors.csv in the same directory.

The contents of vector.csv should be the same each time you run GenVec with the same runtime parameters.
If this is not the case, please let us know.