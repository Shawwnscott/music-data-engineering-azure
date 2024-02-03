-- Create Artist table
CREATE TABLE Artist (
    ArtistID VARCHAR(255) PRIMARY KEY,
    Name VARCHAR(255) NOT NULL,
    Followers INT
);


-- Create Genre table
CREATE TABLE Genre (
    GenreID INT PRIMARY KEY,
    Name VARCHAR(255) NOT NULL
);

-- Create Artist_Genre table
CREATE TABLE Artist_Genre (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    ArtistID VARCHAR(255),
    GenreID INT,
    FOREIGN KEY (ArtistID) REFERENCES Artist(ArtistID),
    FOREIGN KEY (GenreID) REFERENCES Genre(GenreID)
);

-- Create Album table
CREATE TABLE Album (
    AlbumID VARCHAR(255) PRIMARY KEY,
    AlbumTitle VARCHAR(255) NOT NULL,
    AlbumType VARCHAR(255),
    ReleaseDate DATE,
    TotalTracks INT
);

-- Create Artist_Album table
CREATE TABLE Artist_Album (
    ID INT PRIMARY KEY,
    ArtistID VARCHAR(255),
    AlbumID VARCHAR(255),
    FOREIGN KEY (ArtistID) REFERENCES Artist(ArtistID),
    FOREIGN KEY (AlbumID) REFERENCES Album(AlbumID)
);


-- Create Song table
CREATE TABLE Song (
    SongID VARCHAR(255) PRIMARY KEY,
    TrackNumber INT,
    Title VARCHAR(255) NOT NULL,
    AlbumID VARCHAR(255),
    DurationInSeconds INT,
    FOREIGN KEY (AlbumID) REFERENCES Album(AlbumID)
);


-- Create WeeklyDownloads table
CREATE TABLE WeeklyDownloads (
    WeekID INT,
    SongID VARCHAR(255),
    WeekStartDate DATE,
    WeekEndDate DATE,
    TotalWeeklyDownloads INT,
    PRIMARY KEY (WeekID, SongID),
    FOREIGN KEY (SongID) REFERENCES Song(SongID)
);

-- Create WeeklyStreams table
CREATE TABLE WeeklyStreams (
    WeekID INT,
    SongID VARCHAR(255),
    WeekStartDate DATE,
    WeekEndDate DATE,
    TotalWeeklyStreams INT,
    PRIMARY KEY (WeekID, SongID),
    FOREIGN KEY (SongID) REFERENCES Song(SongID)
);
