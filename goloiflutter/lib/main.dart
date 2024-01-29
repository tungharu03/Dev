import 'package:flutter/material.dart';
import 'package:goloiflutter/my_object.dart';

void main() {
  runApp(MyApp());
}

class MyApp extends StatefulWidget {
  @override
  _MyAppState createState() => _MyAppState();
}

class _MyAppState extends State<MyApp> {
  MyObject _myObject = MyObject(value: 0);

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      home: Scaffold(
        appBar: AppBar(
          title: Text('Flutter App'),
        ),
        body: Center(
          child: Column(
            mainAxisAlignment: MainAxisAlignment.center,
            children: [
              Text('Value: ${_myObject.value}'),
              SizedBox(height: 20),
              Row(
                mainAxisAlignment: MainAxisAlignment.center,
                children: [
                  ElevatedButton(
                    onPressed: () {
                      setState(() {
                        _myObject.increase();
                      });
                    },
                    child: Text('Increase'),
                  ),
                  SizedBox(width: 20),
                  ElevatedButton(
                    onPressed: () {
                      setState(() {
                        _myObject.decrease();
                      });
                    },
                    child: Text('Decrease'),
                  ),
                  SizedBox(width: 20),
                  ElevatedButton(
                    onPressed: () {
                      setState(() {
                        _myObject.value = 10;
                      });
                    },
                    child: Text('Set to 10'),
                  ),
                ],
              ),
            ],
          ),
        ),
      ),
    );
  }
}
