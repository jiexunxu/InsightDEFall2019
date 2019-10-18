BOSA (Boxable Open-image Selector & Augmentor)

Google’s open images dataset is a popular choice for data scientists, professors and other machine-learning professionals. Within this dataset, there are over 500GB of images that contains boxable objects such as apples, dogs, laptops etc. The boxes’ locations and sizes, which cover these boxable objects, are saved in an accompanying metadata document. These boxes metadata are verified either by Google or by other sources. As of open images dataset V5, there are over 1 million such images and over 15 million boxes.

This boxable image dataset is valuable for data scientists that wants to train their specially tailored machine-learning models in object detection, recognition etc. Often they will select a subset of the images containing objects that are relevant to their model, augment the images themselves by applying various image transforms such as flip, rotation, scale, crop and blur, and use the resulting data to train their models.

However, no such convenient tool is publicly available that can select and augment the images based on different users’ inputs, and automatically calculate the boxes locations and sizes after various transforms. Data scientists often need to write their own code to do all of the steps above, costing their and their company’s precious time and resources. At Insight, I built BOSA (Boxable Open-image Selector & Augmentor), a scalable system that does all of the data preparations above for multiple users.

BOSA exposes a web UI interface to users. Within the interface, the users can select images based on the type of image labels they are interested in, the number of boxable objects within the image, and whether they only want images data that is verified by google only. The users can augment the images by setting the desired size they want their image to resize to (typically a power of 2 for deep learning), the scaling factor to scale and crop their images, the crop parameters that crops a section of the images, and Gaussian blur parameters to blur the images. The users then clicks the submit button, and BOSA will do the hard work. Once the job is done, BOSA emails the user with instruction for downloading both the processed images and the bounding boxes metadata. Following is an example image data, after the transforms, and the associated metadata.

![sample data](/images/data.jpg)

Multiple users from different places can submit their requests. BOSA will queue up these requests, process them, and notify the users through email. The image below shows the BOSA UI. Note that depending on the job size a user submits, he/she can get two different type of emails. This will be discussed below.

![BOSA UI](/images/UI.jpg)

The architecture of BOSA is illustrated in the following image. At its core, BOSA is powered by AWS EC2 instances that runs Spark. It exposes a Flask based Web UI for user to submit their requests. Once a request is received, BOSA pulls images from S3 and related metadata from a PostgreSQL database, computes the updated bounding boxes, save them to S3, and transforms and saves the images to S3. Once the job is completed an email is sent to the user to notify them.

![BOSA architecture](/images/architecuture.jpg)

There are a few under the hood implementations for BOSA. For small scale requests that involves less than 200 images, BOSA will process and save the images locally with openCV, zip them up and upload to AWS S3 as a zip file for user to download. For very small scale requests that involve only around 10 images, BOSA can do all these steps in a few seconds. This is useful for users to get a quick feeling of how the system works and how the data looks like before submitting their larger scale requests. For large scale requests, BOSA runs Spark on a cluster of AWS EC2 instances, saves the processed images directly onto AWS S3, and let user sync their data with aws cli. A PostgreSQL database is precomputed that can be used by BOSA to quickly extract the image names and their associated bounding boxes metadata. Even with a user request of 16000 images (generally sufficient for transfer learning) that involve over 200,000 bounding boxes, it only takes BOSA less than 10 seconds to retrieve, calculate, and save the bounding boxes metadata, and the subsequent image processing only takes about 30 minutes. For requests involving around 5000 images, metadata computation can be done in around 2 seconds and image processing can be done in about 10 minutes. Following is a quick look of what the saved data looks like on AWS S3 with both a small scale request and a large scale request:

![AWS sample](/images/aws.jpg)

Furthermore, for each email address, BOSA keeps track of the history of parameter choices for that email. This is useful for data scientists for reproducible machine learning purposes. Yet another small feature of BOSA is that it uses boto3 to automatically stop the EC2 instances if there are currently no requests in the system, saving operational cost when BOSA is idle. When requests are present, the stopped EC2 instances are started for faster processing.

Finally, following is a youtube video that demos the BOSA interface:

![Demo video](https://img.youtube.com/vi/pGKgGYeqbCY/0.jpg)
(https://www.youtube.com/watch?v=pGKgGYeqbCY)
