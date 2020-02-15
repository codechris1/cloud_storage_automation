import cv2
 
# read image as grey scale
img = cv2.imread('tmp/DSC03807.JPG')
# get image height, width
(h, w) = img.shape[:2]
# calculate the center of the image
center = (w / 2, h / 2)
 
angle90 = 90
angle180 = 180
angle270 = 270
 
scale = 1.0
 
# Perform the counter clockwise rotation holding at the center
# 90 degrees
M = cv2.getRotationMatrix2D(center, angle90, scale)
rotated90 = cv2.warpAffine(img, M, (h, w))
 
# 180 degrees
M = cv2.getRotationMatrix2D(center, angle180, scale)
rotated180 = cv2.warpAffine(img, M, (w, h))
 
# 270 degrees
M = cv2.getRotationMatrix2D(center, angle270, scale)
rotated270 = cv2.warpAffine(img, M, (h, w))

cv2.imwrite('tmp/DSC03807_rotated90.JPG',rotated90)
cv2.imwrite('tmp/DSC03807_rotated180.JPG',rotated180)
cv2.imwrite('tmp/DSC03807_rotated270.JPG',rotated270)
 
''' 
cv2.imshow('Original Image',img)
cv2.waitKey(0) # waits until a key is pressed
cv2.destroyAllWindows() # destroys the window showing image
 
cv2.imshow('Image rotated by 90 degrees',rotated90)
cv2.waitKey(0) # waits until a key is pressed
cv2.destroyAllWindows() # destroys the window showing image
 
cv2.imshow('Image rotated by 180 degrees',rotated180)
cv2.waitKey(0) # waits until a key is pressed
cv2.destroyAllWindows() # destroys the window showing image
 
cv2.imshow('Image rotated by 270 degrees',rotated270)
cv2.waitKey(0) # waits until a key is pressed
cv2.destroyAllWindows() # destroys the window showing image
'''