DROP TABLE IF EXISTS `article`;
CREATE TABLE `article` (
  `timestamp` char(14) DEFAULT NULL,
  `id` char(7) DEFAULT NULL,
  `aid` char(7) DEFAULT NULL,
  `title` char(15) DEFAULT NULL,
  `category` char(11) DEFAULT NULL,
  `abstract` char(30) DEFAULT NULL,
  `articleTags` char(14) DEFAULT NULL,
  `authors` char(13) DEFAULT NULL,
  `language` char(3) DEFAULT NULL,
  `text` char(31) DEFAULT NULL,
  `image` char(32) DEFAULT NULL,
  `video` char(32) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOCK TABLES `article` WRITE;
INSERT INTO `article` VALUES
  ("1506000000000", "a0", "0", "title0", "technology", "abstract of article 0", "tags25", "author982", "en", "text_a0.txt", "image_a0_0.jpg,", "video_a0_video.flv"),
  ("1506000000001", "a1", "1", "title1", "science", "abstract of article 1", "tags21", "author1239", "en", "text_a1.txt", "image_a1_0.jpg,image_a1_1.jpg,", ""),
  ("1506000000002", "a2", "2", "title2", "technology", "abstract of article 2", "tags12", "author647", "zh", "text_a2.txt", "image_a2_0.jpg,", ""),
  ("1506000000003", "a3", "3", "title3", "science", "abstract of article 3", "tags7", "author174", "en", "text_a3.txt", "image_a3_0.jpg,", ""),
  ("1506000000004", "a4", "4", "title4", "science", "abstract of article 4", "tags26", "author1953", "en", "text_a4.txt", "image_a4_0.jpg,", ""),
  ("1506000000005", "a5", "5", "title5", "science", "abstract of article 5", "tags29", "author1815", "en", "text_a5.txt", "image_a5_0.jpg,image_a5_1.jpg,", ""),
  ("1506000000006", "a6", "6", "title6", "technology", "abstract of article 6", "tags24", "author1580", "en", "text_a6.txt", "image_a6_0.jpg", ""),
  ("1506000000007", "a7", "7", "title7", "science", "abstract of article 7", "tags5", "author1035", "zh", "text_a7.txt", "image_a7_0.jpg", ""),
  ("1506000000008", "a8", "8", "title8", "science", "abstract of article 8", "tags26", "author239", "zh", "text_a8.txt", "image_a8_0.jpg", ""),
  ("1506000000009", "a9", "9", "title9", "technology", "abstract of article 9", "tags1", "author124", "zh", "text_a9.txt", "image_a9_0.jpg,", "");
UNLOCK TABLES;


