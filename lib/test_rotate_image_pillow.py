from PIL import Image

def rotate_image(infile, outfile, degrees):
    """Rotate an image by a number of degrees, crop if desired, and save to outfile"""
    Image.open(infile).rotate(degrees, expand=True, resample=Image.BILINEAR).save(outfile)

infile = "tmp/DSC03807.JPG"
outfile = "tmp/DSC03807_rotated.JPG"

rotate_image(infile, outfile, 90)