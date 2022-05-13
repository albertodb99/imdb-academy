package co.empathy.academy.imdb.utils;

import co.empathy.academy.imdb.exceptions.IndexNotFoundException;
import co.empathy.academy.imdb.model.Film;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TsvDeleter {
    private static final int IS_ADULT = 4;
    private static final int START_YEAR = 5;

    private static final int NCONST = 0;
    private static final int PRIMARY_NAME = 1;
    private static final int BIRTHYEAR = 2;
    private static final int DEATHYEAR = 3;
    private static final int PRIMARY_PROFESSION = 4;
    private static final int KNOWN_FOR_TITLES = 5;
    private static final Pattern DELIMITER = Pattern.compile("\t");


    public void tsvDeleter(String filePath) throws IOException {
        BufferedReader br = null;
        BufferedWriter bw = null;
        FileWriter fw = null;
        String film = "";
        List<String> films = new ArrayList<>();

        br = new BufferedReader(new FileReader(filePath));
        boolean isFirst = true;
        while ((film = br.readLine()) != null) {
            String[] filmSplitted = film.split("\t");
            if(checkLineIsValid(filmSplitted) || isFirst){
                films.add(film);
                isFirst = false;
            }
        }
        File file = new File("title.basics.sorted.cleaned.tsv");
        fw = new FileWriter(file);
        bw = new BufferedWriter(fw);
        for (String filmParsed : films) {
            bw.write(filmParsed);
            bw.newLine();
        }
        br.close();
        bw.close();
    }

    public void cleanRating(String filmsPath, String ratingsPath, String outputPath) throws IOException{
        try(
                BufferedWriter outputWriter = new BufferedWriter(new FileWriter(outputPath));
                BufferedReader filmsReader = new BufferedReader(new FileReader(filmsPath));
                BufferedReader ratingsReader = new BufferedReader(new FileReader(ratingsPath));
        ) {
            String filmsLine = filmsReader.readLine();
            String ratingsLine = ratingsReader.readLine();
            outputWriter.write(ratingsLine);
            outputWriter.newLine();
            filmsLine = filmsReader.readLine();
            ratingsLine = ratingsReader.readLine();

            while(filmsLine != null && ratingsLine != null) {
                var filmLineArray = filmsLine.split(DELIMITER.pattern());
                var ratingLineArray = ratingsLine.split(DELIMITER.pattern());

                var filmIndex = Integer.valueOf(filmLineArray[0].split("tt")[1]);
                var ratingIndex = Integer.valueOf(ratingLineArray[0].split("tt")[1]);

                var filmIndexCorrected = filmLineArray[0];

                if(filmIndex.equals(ratingIndex) ) {
                    outputWriter.write(ratingsLine);
                    outputWriter.newLine();
                    filmsLine = filmsReader.readLine();
                    ratingsLine = ratingsReader.readLine();
                } else if(filmIndex < ratingIndex) {
                    outputWriter.write(filmIndexCorrected);
                    outputWriter.write("\t0.0\t0\t");
                    outputWriter.newLine();
                    filmsLine = filmsReader.readLine();
                } else {
                    ratingsLine = ratingsReader.readLine();
                }
            }
        }
    }

    public void cleanCrew(String filmsPath, String crewsPath, String outputPath) throws IOException{
        try(
                BufferedWriter outputWriter = new BufferedWriter(new FileWriter(outputPath));
                BufferedReader filmsReader = new BufferedReader(new FileReader(filmsPath));
                BufferedReader crewsReader = new BufferedReader(new FileReader(crewsPath));
        ) {
            String filmsLine = filmsReader.readLine();
            String crewsLine = crewsReader.readLine();
            outputWriter.write(crewsLine);
            outputWriter.newLine();
            filmsLine = filmsReader.readLine();
            crewsLine = crewsReader.readLine();

            while(filmsLine != null && crewsLine != null) {
                var filmLineArray = filmsLine.split(DELIMITER.pattern());
                var crewLineArray = crewsLine.split(DELIMITER.pattern());

                var filmIndex = Integer.valueOf(filmLineArray[0].split("tt")[1]);
                var crewIndex = Integer.valueOf(crewLineArray[0].split("tt")[1]);

                var filmIndexCorrected = filmLineArray[0];

                if(filmIndex.equals(crewIndex) ) {
                    outputWriter.write(crewsLine);
                    outputWriter.newLine();
                    filmsLine = filmsReader.readLine();
                    crewsLine = crewsReader.readLine();
                } else if(filmIndex < crewIndex) {
                    outputWriter.write(filmIndexCorrected);
                    outputWriter.write("\t\\N\t\\N\t");
                    outputWriter.newLine();
                    filmsLine = filmsReader.readLine();
                } else {
                    crewsLine = crewsReader.readLine();
                }
            }
        }
    }

    public void cleanEpisodes(String filmsPath, String episodesPath, String outputPath) throws IOException{
        try(
                BufferedWriter outputWriter = new BufferedWriter(new FileWriter(outputPath));
                BufferedReader episodesReader = new BufferedReader(new FileReader(episodesPath));
        ) {
            String episodesLine = episodesReader.readLine();
            outputWriter.write(episodesLine);
            outputWriter.newLine();
            episodesLine = episodesReader.readLine();

            Map<Integer, Film> filmsMap = Files.readAllLines(Paths.get(filmsPath).toAbsolutePath())
                    .stream()
                    .skip(1)
                    .map(Film::new)
                    .collect(Collectors.toMap(Film::getIdToCompare, Film::getIdentity));
            while(episodesLine != null) {
                var episodeLineArray = episodesLine.split(DELIMITER.pattern());
                var episodeIndex = Integer.valueOf(episodeLineArray[0].split("tt")[1]);


                if(filmsMap.get(episodeIndex) != null ) {
                    outputWriter.write(episodesLine);
                    outputWriter.newLine();
                }

                episodesLine = episodesReader.readLine();
            }
        }
    }

    public void cleanNames(String filmsPath, String namesPath, String outputPath) throws IOException{
        try(
                BufferedWriter outputWriter = new BufferedWriter(new FileWriter(outputPath));
                BufferedReader namesReader = new BufferedReader(new FileReader(namesPath));
        ) {
            String namesLine = namesReader.readLine();
            outputWriter.write(namesLine);
            outputWriter.newLine();
            namesLine = namesReader.readLine();

            Map<Integer, Film> filmsMap = Files.readAllLines(Paths.get(filmsPath).toAbsolutePath())
                    .stream()
                    .skip(1)
                    .map(Film::new)
                    .collect(Collectors.toMap(Film::getIdToCompare, Film::getIdentity));
            while(namesLine != null) {
                var nameLineArray = namesLine.split(DELIMITER.pattern());
                var knownForTitlesArray = nameLineArray[KNOWN_FOR_TITLES];
                var knownForTitlesSplit = knownForTitlesArray.split(",");

                int counter = 0;
                StringBuilder titleBuilder = createParsedNameLine(nameLineArray);

                for(String title : knownForTitlesSplit){
                    if(!title.equals("\\N")){
                        int titleIndex = 0;
                        titleIndex = Integer.parseInt(title.split("tt")[1]);

                        if(filmsMap.get(titleIndex) != null ) {
                            if(counter == 0)
                                titleBuilder.append(title);
                            else
                                titleBuilder.append(",").append(title);
                            counter++;
                        }
                    }
                }

                if(counter > 0){
                    outputWriter.write(titleBuilder.toString());
                    outputWriter.newLine();
                }

                namesLine = namesReader.readLine();
            }
        }
    }

    private StringBuilder createParsedNameLine(String[] nameLineArray) {
        StringBuilder titleBuilder = new StringBuilder();
        titleBuilder.append(nameLineArray[NCONST]);
        titleBuilder.append("\t");
        titleBuilder.append(nameLineArray[PRIMARY_NAME]);
        titleBuilder.append("\t");
        titleBuilder.append(nameLineArray[BIRTHYEAR]);
        titleBuilder.append("\t");
        titleBuilder.append(nameLineArray[DEATHYEAR]);
        titleBuilder.append("\t");
        titleBuilder.append(nameLineArray[PRIMARY_PROFESSION]);
        titleBuilder.append("\t");
        return titleBuilder;
    }

    private boolean checkLineIsValid(String[] filmSplitted) {
        return !(checkAdultFilm(filmSplitted[IS_ADULT])
                || checkStartYear(filmSplitted[START_YEAR]));
    }

    private boolean checkAdultFilm(String adultFilm){
        return adultFilm.equals("\\N") || adultFilm.equals("1");
    }

    private boolean checkStartYear(String startYear){
        int startYearParsed;
        try{
            startYearParsed = Integer.parseInt(startYear);
        }catch(NumberFormatException e){
            startYearParsed = 0;
        }
        return startYearParsed <= 1970;
    }
}

