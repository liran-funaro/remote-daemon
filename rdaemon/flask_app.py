"""
Author: Liran Funaro <funaro@cs.technion.ac.il>

Copyright (C) 2006-2018 Liran Funaro

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""
import threading

from zope.interface.declarations import implementer

from rdaemon.daemons import BaseDaemon
from rdaemon.interfaces import IDaemon


@implementer(IDaemon)
class FlaskAppDaemon(BaseDaemon):
    """
    Flask app daemon
    """

    def __init__(self, app, host, port, debug=False, **options):
        super().__init__()
        self.app = app
        self.host = host
        self.port = port
        self.debug = debug
        self.options = options
        self.options["use_reloader"] = False

        self.flask_thread = threading.Thread(target=self.app_run, daemon=True)

    def run(self):
        """
        Runs the app thread and waits for termination
        """
        self.flask_thread.start()
        super(FlaskAppDaemon, self).run()

    def app_run(self):
        """
        Starts the app. We use this option although it is not recommended.
        See flask deployment options for more information:
           http://flask.pocoo.org/docs/0.12/deploying/
        "While lightweight and easy to use, Flask’s built-in server is not suitable for production as
        it doesn’t scale well and by default serves only one request at a time.
        Some of the options available for properly running Flask in production are documented here.
        If you want to deploy your Flask application to a WSGI server not listed here, look up the
        server documentation about how to use a WSGI app with it.
        Just remember that your Flask application object is the actual WSGI application."

        See Flask.run() and run_simple() for the parameters documentation
        """
        self.app.run(host=self.host, port=self.port, debug=self.debug,
                     # threaded=self.threaded, processes=self.processes,
                     **self.options)
