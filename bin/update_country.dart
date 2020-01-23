import 'dart:io';
import 'dart:convert';
import 'package:postgres/postgres.dart';
import 'package:csv/csv.dart';

main(List<String> arguments) async {
  Stopwatch stopwatch = Stopwatch()..start();

  final input = File('./import/geoTLD.csv').openRead();
  final rows = await input
      .transform(utf8.decoder)
      .transform(CsvToListConverter(
          eol: "\n", fieldDelimiter: ",", textDelimiter: '"'))
      .toList();

  var connection = PostgreSQLConnection("localhost", 5432, "b2b_client",
      username: "kenbo", password: "kenbo");
  await connection.open();

  for (var row in rows) {
    print('tld:${row[0]}, country:${row[3]}');
    //делаем SELECT по TLD
    String tld = row[0];
    String valueUpdate = row[3];

    var resultSelect = await connection.query(
          "SELECT id FROM bigdomain WHERE domain_rev LIKE '$tld.%' ",
          timeoutInSeconds: 150);
    if (resultSelect == null) { break; }
    print('Найдено Rows: ${resultSelect.length}\n');
    //print(resultSelect[0]);
    
    int sizePacket = 10;
    int countRows = resultSelect.length;
    int countPackets = (countRows / sizePacket).ceil();

    print(
      'Замеры для Update:\n -sizePacket:$sizePacket\n -countRows: $countRows\n -countPackets:$countPackets\n');

    

    //TODO запускаем цикл по каждому пакету
    await connection.transaction((ctx) async {
      for (var i = 0; i < countPackets; i++) {
        //if (i == 1) break;
        print('Packet#$i:');
        int sizePacketExt =
            i != countPackets - 1 ? (i * sizePacket) + sizePacket : countRows;

        //print('${i*sizePacket}, $sizePacketExt');
        var subListId = resultSelect.sublist(i * sizePacket, sizePacketExt);

        List<String> taskMap = [];
        subListId.forEach((id) {
          taskMap.add('($id, $valueUpdate)');
        });

        String bigStringForUpdQuery = taskMap.join(",");

        Stopwatch stopwatch3 = Stopwatch()..start();
        await ctx.query(
            "UPDATE bigdomain as b SET country_name = c.value_country_name FROM (values $bigStringForUpdQuery) as c(id, value_country_name) WHERE c.id = b.id",
            timeoutInSeconds: 150);
        print('- UPDATE time: ${stopwatch3.elapsed}');

      } //end for
    
    }).catchError((e) {
      print(e);
      connection.cancelTransaction();
    });

    break;
  }

  print('main(): ${stopwatch.elapsed}');
  await connection.close();
}
