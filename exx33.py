def looplistappend(iterator1,n):


    i = 0
    numbers = []
    #iterator1 = int(raw_input("put in yout fucking circle num:>"))
    print iterator1
    while i < iterator1:
        print "At the top i is %d" %i
        numbers.append(i)

        i = i + n
        print "Numbers now:",numbers
        print "At the bottom i is %d" % i



    print "The numbers:"

    for num in numbers:
        print num

a1 = int(raw_input("num of iterate:"))
a2 = int(raw_input("input the interval of increase:"))
looplistappend(a1,a2)
