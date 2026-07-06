CREATE OR REPLACE FUNCTION customer.create_project_trigger()
RETURNS TRIGGER AS $$
DECLARE
    role_id UUID; 
BEGIN
  -- Get the role_id of the user
  SELECT id 
  INTO role_id 
  FROM customer.role 
  WHERE name = 'project-owner';

  -- Insert a new row into customer.project_user table when a row is added to customer.project table
  INSERT INTO customer.project_user (project_id, user_id, role_id)
  VALUES (NEW.id, NEW.user_id, role_id);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER add_project_user_trigger
AFTER INSERT ON customer.project
FOR EACH ROW
EXECUTE FUNCTION customer.create_project_trigger();