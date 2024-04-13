from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
import os
import tempfile

site_url = "https://kpmgindia365-my.sharepoint.com"
username = "shrimantas@kpmg.com"
password = "Shridipta06@"

# Authenticate with SharePoint
ctx_auth = ClientContext(site_url).with_credentials(UserCredential(username, password))


file_url = "/Shared Documents/MasterSchema.txt"
download_path = os.path.join(tempfile.mkdtemp(), os.path.basename(file_url))
with open(download_path, "wb") as local_file:
    file = (
        ctx_auth.web.get_file_by_server_relative_url(file_url)
        .download(local_file)
        .execute_query()
    )
print("[Ok] file has been downloaded into: {0}".format(download_path))