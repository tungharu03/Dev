import 'package:flutter/material.dart';

void main() {
  runApp(const MyApp());
}

class MyApp extends StatelessWidget {
  const MyApp({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return const MaterialApp(
      home: HomePage(),
    );
  }
}

class HomePage extends StatefulWidget {
  const HomePage({Key? key}) : super(key: key);

  @override
  State<HomePage> createState() => _HomePageState();
}

class _HomePageState extends State<HomePage> {
  int value = 0;

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        leading: const Icon(Icons.arrow_back_outlined),
        title: const Text('My App'),
        backgroundColor: Colors.lightBlueAccent,
        actions: const [
          Icon(Icons.notifications),
          Icon(Icons.settings),
        ],
      ),
      body: Center(
        child: Column(
          mainAxisAlignment: MainAxisAlignment.center,
          children: [
            const Text(
              'Value',
              style: TextStyle(
                fontSize: 24,
              ),
            ),
            Row(
              mainAxisAlignment: MainAxisAlignment.center,
              children: [
                IconButton(
                  onPressed: () {
                    // Giảm giá trị khi nhấn nút remove
                    setState(() {
                      value -= 1;
                    });
                  },
                  icon: const Icon(Icons.remove),
                ),
                Text(
                  '$value',
                  style: const TextStyle(
                    fontSize: 24,
                  ),
                ),
                IconButton(
                  onPressed: () {
                    // Tăng giá trị khi nhấn nút add
                    setState(() {
                      value += 1;
                    });
                  },
                  icon: const Icon(Icons.add),
                ),
              ],
            ),
          ],
        ),
      ),
    );
  }
}
