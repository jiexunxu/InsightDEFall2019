def batch_save(connection, imageids):
    cursor=connection.cursor()

    def save_bbox_metadata():
        query="SELECT * FROM image_bbox WHERE imageid IN %s"
        cursor.execute(query)
        all_bbox_descriptors=cursor.fetchall()
