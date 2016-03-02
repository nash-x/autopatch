# autopatch
Welcome to the autopatch

#Introduction
autopatch will help you to copy files in your patches to site-packages path.

#Structure
The directory structure of autopatch is as follows:


    autopatch

    -backup

    -patches

    -__init__.py

#How to use
What user need to do is put user's patches in "patches" folder, and run command:

    # python __init__.py


# What it to do when run autopatch
when run it, it will do following things:


1.Validate if patches conflict with each other.
If validate failed, it will print confict detail info, and exit.
If validate pass, it will do follows steps.

2.Backup your code of service which you want to patch, like "copy -rf /usr/lib64/python2.6/site-package/nova .../autopatch/backup".


3.Copy files in your patches to site-packages folder, like "copy -rf <YOUR_PATCH> /usr/lib64/python2.6/site-package/"

