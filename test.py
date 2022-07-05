class Hello:
    def __init__(self, func):
        self.func = func

    def callback_hello(self):
        print("1")
        self.func()


def hello():
    print("Hello,world!")


t1 = Hello(hello)
t1.callback_hello()

