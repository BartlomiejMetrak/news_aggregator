/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `sources_news_rss`
--

DROP TABLE IF EXISTS `sources_news_rss`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `sources_news_rss` (
  `id` int NOT NULL,
  `source` varchar(200) NOT NULL,
  `base_link` varchar(200) NOT NULL,
  `block_rss` tinyint(1) NOT NULL DEFAULT '0',
  `rss_link` varchar(400) NOT NULL,
  `art_download` tinyint(1) NOT NULL DEFAULT '0',
  `img_download` tinyint(1) NOT NULL DEFAULT '0',
  `stock_news_multi` tinyint(1) NOT NULL DEFAULT '0',
  `feed_companies` tinyint(1) NOT NULL DEFAULT '0',
  `point_score` tinyint NOT NULL DEFAULT '0',
  `article_error_daily` int NOT NULL DEFAULT '0',
  `article_error_max` int NOT NULL,
  `updated_at` date NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='0 - download, 1 - do not download, dla fxmag nie pobieramy artykułu bo pobierany jest on z rss feed, stock_news_multi - w których gazetach możemy szukać omówień dnia giełdowego (1 - szukamy)\n\nDołączyć:\n- business insider\n- wiadomości radio zet\n- w Polityce\n- RP\n- Parkiet';
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
