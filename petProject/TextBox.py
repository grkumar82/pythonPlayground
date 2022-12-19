'''
I'm implementing two functions within a single class called textModify. 
This class takes a word and a delimiter (also a string) and outputs a string in a certain format. 
There are two functions within the class which when invoked will modify the output. 
One of the functions (referred to as addBelow) adds specified number of empty lines while the other (referred to as addRight) 
add specified number of empty characters to the right of word. The key requirement is calling addRight and 
addBelow shouldn't mutate the original box object.

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
'''


class TextModify:

    def __init__(self, word, delimit, padbelow=0, padright=0):
        self.word = word
        self.delimit = delimit
        self.padbelow = padbelow
        self.padright = padright

    def addBelow(self, num1):
        return TextModify(self.word, self.delimit, num1, self.padright)

    def addRight(self, num2):
        return TextModify(self.word, self.delimit, self.padbelow, num2)

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
        for i in range(len(output)):
            print(output[i])


test = TextModify('Welcome', '+', 0)
test.addRight(2).addBelow(1).show()
# output
'''
+++++++++++++++
 + Welcome   + 
             
+++++++++++++++
'''
test.addRight(2).addBelow(2).show()
'''
+++++++++++++++
 + Welcome   + 
             
             
+++++++++++++++
'''
test.addRight(2).show()
'''
+++++++++++++++
 + Welcome   + 
+++++++++++++++
'''
test.addBelow(4).show()
'''
+++++++++++++++
 + Welcome   + 
             
             
             
             
+++++++++++++++
'''
test.addRight(4).show()
'''
+++++++++++++++++
 + Welcome     + 
             
             
             
             
+++++++++++++++++
'''
test.addRight(2).addBelow(0).show()
'''
+++++++++++++++
 + Welcome   + 
+++++++++++++++
'''
test.addRight(2).addBelow(1).show()
'''
+++++++++++++++
 + Welcome   + 
             
+++++++++++++++
'''
test.addRight(5).show()
'''
++++++++++++++++++
 + Welcome      + 
             
             
             
             
++++++++++++++++++
'''
test.addBelow(4).show()
'''
++++++++++++++++++
 + Welcome      + 
             
             
             
             
++++++++++++++++++
'''

