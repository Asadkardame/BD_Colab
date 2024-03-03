list1=[1,2,3,4,5]

size1=len(list1)
print(size1)

a=list1[size1-1]
b=list1[0]
list1[0]=a
list1[size1-1]=b
print(list1)