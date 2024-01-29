class MyObject {
  int value;

  MyObject({
    required this.value,
  });

  // Phương thức tăng giá trị
  void increase() {
    value++;
  }

  // Phương thức giảm giá trị
  void decrease() {
    value--;
  }

  // Phương thức trả về giá trị
  int get getValue => value;

  // Phương thức đặt giá trị
  set setValue(int newValue) {
    value = newValue;
  }
}