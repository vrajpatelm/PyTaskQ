@app.post("/task/send_email"):

--""email:str = Form(...),
    title: str = Form(...),
    body: str = Form(...)
    ""
    This "= Form(...)" part tell FastAPi  to extract Data from FORM field and "..."(3-dot) is there to tell FastAPi that this Field is required.
    - This all for better User exprence and Better type Validation

-- task email request is waited (form = await request.form()) and if scuessfull then a new uuid (unique identifiaction) is assigned to TASK and A var named task is to sturecd it and pushed to redis list .

-- last return status to user as Queued


@app.get("/task/matrix_multiply")

-- i validate that if no arg size is passed it is by deafult 10 