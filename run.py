from main import create_app


app = create_app('config.development')

if __name__ == '__main__':
    app.run()
