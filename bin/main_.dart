import 'dart:async';
import 'dart:io';
import 'dart:convert';
import 'package:postgres/postgres.dart';
import 'package:csv/csv.dart';

main(List<String> arguments) async {
  Stopwatch stopwatch = Stopwatch()..start();

  final input = File('./import/quantcast-usa.csv').openRead();
  final rows = await input
      .transform(utf8.decoder)
      .transform(CsvToListConverter(
          eol: "\n", fieldDelimiter: ",", textDelimiter: '"'))
      .toList();
  //print(rows.length);
  //File fileOutput = File('./output/alexaInsert.txt');

  var connection = PostgreSQLConnection("localhost", 5432, "b2b_client",
      username: "kenbo", password: "kenbo");

  await connection.open();

  int sizePacket = 25000;
  String columnForUpdate = 'quantcast';
  int countRows = rows.length;
  int countPackets = (countRows / sizePacket).ceil();

  print(
      'Замеры:\n -sizePacket:$sizePacket\n -countRows: $countRows\n -countPackets:$countPackets\n');

  await connection.transaction((ctx) async {
    for (var i = 0; i < countPackets; i++) {
      //if (i == 1) break;
      print('Packet#$i:');
      int sizePacketExt =
          i != countPackets - 1 ? (i * sizePacket) + sizePacket : countRows;

      //print('${i*sizePacket}, $sizePacketExt');
      var subRows = rows.sublist(i * sizePacket, sizePacketExt);

      Map<String, dynamic> taskMap = {};
      subRows.forEach((list) {
        taskMap.putIfAbsent(list[1].toString().trim(), () {
          return {
            'valueForUpdate': list[0],
            'tableRowId': null,
          };
        });
      });

      String selectStringMarginWithoutQuotes = taskMap.keys.join("','");
      //print("SELECT id, domain FROM bigdomain WHERE domain IN ('$selectStringMarginWithoutQuotes')");

      Stopwatch stopwatch2 = Stopwatch()..start();
      var result = await ctx.query(
          "SELECT id, domain FROM bigdomain WHERE domain IN ('$selectStringMarginWithoutQuotes')",
          timeoutInSeconds: 150);
      //print(result);
      print('- SELECT time: ${stopwatch2.elapsed}');

      List<String> updateValues = [];
      result.forEach((subList) {
        int tableRowId = subList[0];
        taskMap[subList[1]]['tableRowId'] = tableRowId;
        var valueForUpdate = taskMap[subList[1]]['valueForUpdate'];

        updateValues.add('($tableRowId, $valueForUpdate)');
      });

      //print('$taskMap');
      print('- taskMap elements: ${taskMap.keys.length}');
      print('- updateValues: ${updateValues.length}');

      String bigStringForUpdQuery = updateValues.join(",");

      //print('bigStringForUpdQuery:');
      //print(bigStringForUpdQuery);
      Stopwatch stopwatch3 = Stopwatch()..start();
      //print("UPDATE bigdomain as b SET $columnForUpdate = c.value_$columnForUpdate FROM (values $bigStringForUpdQuery) as c(id, value_$columnForUpdate) WHERE c.id = b.id");
      await ctx.query(
          "UPDATE bigdomain as b SET $columnForUpdate = c.value_$columnForUpdate FROM (values $bigStringForUpdQuery) as c(id, value_$columnForUpdate) WHERE c.id = b.id",
          timeoutInSeconds: 150);

      print('- UPDATE time: ${stopwatch3.elapsed}');

      var notFoundForKeys = taskMap.keys
          .where((key) => taskMap[key]['tableRowId'] == null ? true : false)
          .toList();
      //print(notFoundForKeys);

      //notFoundForKeys.add('thestartmagazine.com.ru.cn');

      List<String> insertValues = [];
      notFoundForKeys.forEach((key) {
        String domain_rev = key.split('.').reversed.toList().join('.');
        insertValues
            .add("('$key', '$domain_rev', ${taskMap[key]['valueForUpdate']})");
      });

      String bigStringForInsertQuery = insertValues.join(",");

      if (bigStringForInsertQuery.isNotEmpty) {
        print('- insertValues: ${insertValues.length}');
        //print("INSERT INTO bigdomain (domain, domain_rev, $columnForUpdate) VALUES $bigStringForInsertQuery");
        //print(taskMap);
        //break;

        Stopwatch stopwatch4 = Stopwatch()..start();
        await ctx.query(
            "INSERT INTO bigdomain (domain, domain_rev, $columnForUpdate) VALUES $bigStringForInsertQuery",
            timeoutInSeconds: 150);
        print('- INSERT time: ${stopwatch4.elapsed}\n');
      }
      print('\n');

      //
      //
    } //end for
  
  }).catchError((e) {
    print(e);
    connection.cancelTransaction();
  });

  //print(domainsByRank2);
  //print(domainsByRank2['go.com']);

  //Создаем экземпляр и пишем в очередь
  //QueryField - queryStr, valueUpdate, taskStatus: work, found, failed
  //FounId

/*
  "SELECT
 id,  
 domain
FROM
   bigdomain
WHERE
   domain IN ( 'mdi-toys.ru', 'toys.ru', 'google.ru')";
*/

/*быстрее должен быть*
1) сначало по ru
2) затем по первым 3-5 буквам: vkon
3) затем по полному соответсвию vkontakte.ru
SELECT
 *
FROM
   (SELECT * FROM bigdomain WHERE domain IN ( 'mdi-toys.ru', 'toys.ru', 'google.ru')" )
WHERE
   tld0 = ru */

/*
update bigdomain as m set
    alexa = c.value_alexa
from (values
    (388, 222),
    (6785883, 222)
) as c(id, value_alexa)
where c.id = m.id;
*/
/*INSERT INTO important_user_table
(id, date_added, status_id)
VALUES
(100, '2015-01-01', 3),
(110, '2015-01-01', 3),
(153, '2015-01-01', 3),
(100500, '2015-01-01', 3);*/
/*
  await connection.transaction((ctx) async {
    var result = await ctx.query("SELECT id, domain FROM bigdomain WHERE domain = 'mdi-toys.ru'");

    print(result);

    await ctx.query("UPDATE bigdomain SET alexa = @alexaRank WHERE id = @id", 
      substitutionValues: {
          "alexaRank" : 123,
          "id": result.last[0]
      });
  });
*/

/*

  int countProcess = 0;
  int testCount = 0;
  Map<String, int> tldsStat = {};
  int thisCount;

  for (var line in lines) {
    // Если государственный или образовательный домен
    if (RegExp(
            r"(.*\.gov$)|(.*\.gov\.[a-z]{1,4}$)|(.*\.go\.(kr|th|my|jp)$)|(.*\.edu$)")
        .hasMatch(line)) {
      continue;
    }

    List<String> tlds = line.split('.').reversed.toList();
    int countPart = tlds.length;
    if (countPart > 2) {
      if (RegExp(
              r"(.*\.net\.[a-z]{1,4}$)|(.*\.org\.[a-z]{1,4}$)|(.*\.com\.[a-z]{1,4}$)|(.*\.(go|or|co|in|ne)\.[a-z]{1,4}$)")
          .hasMatch(line)) {
        if (countPart > 3) {
          // ! Это национальный домен 3-го и более уровня
          // TODO ищем основной домен
          // по '%tld[2].tld[1].tld[0]'
          // если в результате >1 то смотрим какой '%tld[3].tld[2].tld[1].tld[0]'
        } else {
          // ! Это национальный только домен 3-го уровня
          //print(tlds.toString());
          //print('Это основной домен - $line'); // TODO нужно получить Rank
        }
      } else {
        // ! Это любой домен 3-го кроме национальных
        //if (countPart > 3) {print(line);}
        if (tldsStat['${tlds[1]}.${tlds[0]}'] != null) {
          thisCount = tldsStat['${tlds[1]}.${tlds[0]}'];
          tldsStat['${tlds[1]}.${tlds[0]}'] = 1 + thisCount;
        } else {
          tldsStat['${tlds[1]}.${tlds[0]}'] = 1;
        }

        //print(line); // TODO добавить в проверку на Alexa для получения основного домена и РАНК сохраняем
        //print(tlds.toString());
      }
    } else {
      //print(line);

    }
    // !bool hasSkip = checkSkip(line);
    // !if (hasSkip) {continue;}

    //fileOutput.writeAsStringSync('${e.toString()} ${line.trim()}\n', mode: FileMode.append);
  }

  */

  print('main(): ${stopwatch.elapsed}');
  await connection.close();
}
