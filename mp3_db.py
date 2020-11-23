import eyed3
import sqlite3
from pathlib import Path
from datetime import datetime

#******************************************************************
# DATABASE CREATION
#******************************************************************

def loadData():
    #Connect to file
    try:
        conn = sqlite3.connect('myMusic.sqlite')
        cur = conn.cursor()
    except:
        print('Error; failed to connect to file.')
        quit()

    
    #Drop tables if they already existed
    cur.execute('DROP TABLE IF EXISTS SONG')
    cur.execute('DROP TABLE IF EXISTS DISCOGRAPHY')
    cur.execute('DROP TABLE IF EXISTS ARTIST')
    cur.execute('DROP TABLE IF EXISTS ALBUM')

    conn.commit()

    #Create new tables
    cur.execute('''
    CREATE TABLE ALBUM(
        ALBUM_ID INTEGER NOT NULL UNIQUE,
        TITLE VARCHAR(128),
        PRIMARY KEY(ALBUM_ID AUTOINCREMENT)
    )
    ''')
    cur.execute('''
    CREATE TABLE ARTIST(
        ARTIST_ID INTEGER NOT NULL UNIQUE,
        NAME VARCHAR(128),
        PRIMARY KEY(ARTIST_ID AUTOINCREMENT)
    )
    ''')
    cur.execute('''
    CREATE TABLE SONG(
        SONG_ID INTEGER NOT NULL UNIQUE,
        TITLE VARCHAR(128),
        ALBUM_ID INTEGER,
        LENGTH INTEGER,
        CREATION_DATE VARCHAR(30),
        PRIMARY KEY(SONG_ID AUTOINCREMENT)
    );
    ''')
    cur.execute('''
    CREATE TABLE DISCOGRAPHY(
        ARTIST_ID INTEGER,
        SONG_ID INTEGER
    )
    ''')


    conn.commit()

    #******************************************************************
    # UPDATING DATABASE
    #******************************************************************
    def addToDB(song, fileName):    

        #GETTING METADATA
        
        #Check that song tag info is not empty
        if song.tag is None:
            title = fileName[fileName.rfind('\\') + 1:]
            title = title[:title.rfind('.mp3')]
            artist = None
            album = None
            creation_date = None
            length = int(round(song.info.time_secs, 0))

        #Otherwise, extract needed metadata from tag info
        else:
            title = song.tag.title
            #If song does not have a title tag, get it from the original file name
            if title is None or title == '':
                title = fileName[fileName.rfind('\\') + 1:]
                title = title[:title.rfind('.mp3')]

            artist = song.tag.artist
            album = song.tag.album
            creation_date = song.tag.file_info.atime
            #Convert creation date from UNIX time to timestamp
            creation_date = datetime.fromtimestamp(creation_date).strftime('%Y/%m/%d %H:%M:%S')
            length = int(round(song.info.time_secs, 0))
        
        
        #ADDING TO DATABASE

        #Artist

        #Check if no artist was provided
        if artist is None:
            artistID = 'NULL'

        else:
            #See if artist is already in the artist table
            cur.execute('''
                SELECT ARTIST_ID FROM ARTIST WHERE 
                NAME = ?
            ''', (artist,))
            artistID = cur.fetchone()

            #If they are not, add them
            if artistID is None:
                cur.execute('''
                    INSERT INTO ARTIST(NAME)
                    VALUES
                    (?)
                ''', (artist,))

                #Get newly generated ID for future use
                cur.execute('''
                    SELECT ARTIST_ID FROM ARTIST
                    WHERE NAME = ?
                ''', (artist,))

                artistID = cur.fetchone()
        
        conn.commit()


        #Album
        #Check if no album was provided
        if album is None:
            albumID='NULL'
        
        else:
            #See if album is already in the album table
            cur.execute('''
                SELECT ALBUM_ID FROM ALBUM WHERE
                TITLE = ?
            ''', (album,))
            albumID = cur.fetchone()

            #If it is not, add it
            if albumID is None:
                cur.execute('''
                    INSERT INTO ALBUM(TITLE)
                    VALUES
                    (?)
                ''', (album,))

                #Get newly generated ID for future use
                cur.execute('''
                    SELECT ALBUM_ID FROM ALBUM WHERE
                    TITLE = ?
                ''', (album,))
                albumID = cur.fetchone()

        conn.commit()


        #Song
        #Add song to database
        cur.execute('''
            INSERT INTO SONG(TITLE, ALBUM_ID, LENGTH, CREATION_DATE)
            VALUES
            (?, ?, ?, ?)
        ''', (title, albumID[0], length, creation_date))

        #Get song ID for future use
        cur.execute('''
            SELECT SONG_ID FROM SONG
            WHERE TITLE = ? 
        ''', (title,))

        songID = cur.fetchone()

        #Artist-Song Connection
        #Skip this if no artist was provided
        if artistID != 'NULL':
            cur.execute('''
                INSERT INTO DISCOGRAPHY
                VALUES
                (?, ?)
            ''', (artistID[0], songID[0]))

        conn.commit()
        
        #WRITING INFO TO DEBUG FILE
        fileHandle = open('debug.txt', 'a', encoding='utf-8')
        fileHandle.write('Filename: ' + fileName + '\n')

        if title is not None:
            fileHandle.write('Title: ' + title)
        if artist is not None:
            fileHandle.write(' Artist: ' + artist)
        if album is not None:
            fileHandle.write(' Album: ' + album)
        if creation_date is not None:
            fileHandle.write(' Creation date: ' + creation_date)
        if length is not None:
            fileHandle.write(' Length: ' + str(length))

        fileHandle.write('\n')
                
        fileHandle.close()

    #******************************************************************
    # GETTING FILES
    #******************************************************************

    #Get input for folder to scan
    musicFolderName = input('Enter name of music folder: ')
    musicFolder = Path(musicFolderName)

    #Function to find all songs in a folder, including unwrapping folders within 
    #that folder
    def getSongs(songsList, currItem):

        for item in currItem.iterdir():
            #If item is a directory, go get the songs from that directory too
            if item.is_dir():
                getSongs(songsList, item)
            else:
                #Only add to songs list if it is an mp3 file
                if str(item).find('.mp3') != -1:
                    songsList.append(item)
        
        return songsList


    #Get list of all songs in each subfolder of main folder
    #for folder in musicFolder.iterdir():
    #Grab songs
    currFolder = musicFolder
    songsList = []

    songsList = getSongs(songsList, currFolder)

    #Clear debug file
    fileHandle = open('debug.txt', 'w', encoding='utf-8')
    fileHandle.write("")
    fileHandle.close()    

    #Extracting metadata from songs, adding to database
    for song in songsList:

        #Open song file
        songFile = eyed3.load(song)

        #Update database
        addToDB(songFile, str(song))
    
    #Close connection
    conn.close()



#******************************************************************
# QUERYING
#******************************************************************

#Function for formatting second length values from results into minute
#and second format
def formatLength(length):
    mins = str(length // 60)
    secs = str(length % 60)
    if(int(secs) < 10):
        secs = '0' + secs
    
    return (mins + ':' + secs)

#Function for running provided queries on music data    
def runQueries():

    #Database connection
    try:
        conn = sqlite3.connect('myMusic.sqlite')
        cur = conn.cursor()
    except:
        print('Error; could not connect to file.')
        quit()

    
    cur.execute('''
    SELECT ALBUM.TITLE, COUNT(SONG.TITLE)
    FROM ALBUM JOIN SONG JOIN ARTIST
    ON SONG.ALBUM_ID = ALBUM.ALBUM_ID
    WHERE ARTIST.NAME = 'Rap'
    GROUP BY ALBUM.TITLE
    ORDER BY ALBUM.TITLE
    ''')

    print('Result:')
    for row in cur.fetchall():
        print(row)

    conn.commit()




#******************************************************************
# MAIN MENU
#******************************************************************
selection = input('Update data? (Y/N) ')

if selection.upper() == 'Y':
    loadData()
    print('Data updated!')

selection = input('Run queries? (Y/N) ')
if selection.upper() == 'Y':
    runQueries()
    print('Queries ran!')
