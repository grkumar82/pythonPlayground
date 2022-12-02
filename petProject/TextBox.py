'''
box = textModify("welcome", "+")
box.addRight(4).show()
++++++++++++++++
+ welcome     +
++++++++++++++++
box.addBelow(3).show()
++++++++++++
+ welcome +
+         +
+         +
+         +
++++++++++++
box.addRight(4).addBelow(3).show()
++++++++++++++++
+ welcome     +
+             +
+             +
+             +
++++++++++++++++
Calling addRight and addBelow shouldn't mutate the original box object
'''


class TextModify:

    def __init__(self, word, delimit, padbelow=0, padright=0):
        self.word = word
        self.delimit = delimit
        self.padbelow = padbelow
        self.padright = padright

    def reset(self):
        self.padbelow = 0
        self.padright = 0

    def addBelow(self, num1):
        self.reset()
        self.padbelow = num1
        return TextModify(self.word, self.delimit, self.padbelow, self.padright)

    def addRight(self, num2):
        self.reset()
        self.padright = num2
        return TextModify(self.word, self.delimit, self.padbelow, self.padright)

    def show(self):
        output = []
        if self.word:
            if self.padright:
                borderlines = self.delimit * (6 + self.padright + len(self.word))
            else:
                borderlines = self.delimit * (6 + len(self.word))
        else:
            borderlines = self.delimit * 6
        output.append(borderlines)
        if self.padright:
            centerline = ' ' + self.delimit + ' ' + self.word + self.padright * ' ' + ' ' + self.delimit + ' '
        else:
            centerline = ' ' + self.delimit + ' ' + self.word + ' ' + self.delimit + ' '
        output.append(centerline)
        if self.padbelow:
            belowline = ' ' * (6 + len(self.word))
            tmp = self.padbelow
            while tmp:
                output.append(belowline)
                tmp -= 1
        output.append(borderlines)
        print(self.padright, self.padbelow)
        for i in range(len(output)):
            print(output[i])

# These are the commands I ran - 
test = TextModify('Welcome', '+', 0)
test.addRight(2).addBelow(1).show()
test.addRight(2).addBelow(2).show()
test.addRight(2).show()  # working fine
test.addBelow(4).show()  # working fine
test.addRight(4).show(). # working fine
test.addRight(2).addBelow(0).show() # doesn't work correctly
test.addRight(2).addBelow(1).show() # doesn't work correctly
test.addRight(5).show() # working fine
test.addBelow(4).show() # doesn't work correctly

